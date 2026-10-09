<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# footer-decode-bench

Two self-contained, plan-only micro-benchmarks (no I/O, no page decode). Outputs
(CSV + SVG) are written to [`results/`](results/).

- **decode** — resolve `{data_page_offset, total_compressed_size, null_count, min,
  max}` for a column projection across legacy, jump-table, current modular, and page-first
  layouts. `--placement-only` measures the offset-and-size workload from the proposal.
- **name-resolve** — resolve column *names* to ordinals, walking the schema vs. a
  persisted name hash.

The Thrift parser (`thrift_codec.h`) is shared and held constant, so measured gaps
are the *layout*, not parser quality. min/max are returned as zero-copy spans, so
numbers reflect decode work, not the allocator.

## 1. Decode bench

| layout | what it does | cost |
|---|---|---|
| `standard` | materialize the entire FileMetaData into an object tree, then read off it (models parquet-java). | O(whole footer) |
| `walk` | today's nested footer: visit every column chunk, decode the projected ones. | O(all chunks) |
| `index` | jump table: seek straight to each projected chunk. | O(projected) |
| `modular` | `ModularFooter`: column-major bit-packed placement + a separate `ColumnStatistics` module. | O(projected) |
| `page_cold` | page-first footer: decode the mandatory DBP region offsets, then directly access projected columns' fixed-width placement arrays. | O(all regions + projected pages) |
| `page_warm` | page-first footer with the region map retained; placement fields are fixed-width random-access arrays. | O(projected pages) |

Files: `footer_decode_bench.cc` (driver), `footer_formats.h` (the six resolvers),
`plot_sweep.py`, `run_corpus.py`. Placement-only runs cross-check every resolver against the current
footer before timing. Page-first statistics decoding is not part of this benchmark.

```sh
cd /path/to/parquet-footer-bench
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j --target jumptable_footer_convert modular_footer_convert footer_decode_bench
../build/jumptable_footer_convert input.parquet input.jt.parquet
../build/modular_footer_convert   input.parquet input.modular
python3 ../modular-footer/parquet_to_page_modular.py \
  --truncate-minmax 0 input.parquet input.page-modular
../build/footer_decode_bench input.jt.parquet 1 --placement-only \
  input.modular input.page-modular
../build/footer_decode_bench input.jt.parquet --sweep --placement-only \
  input.modular input.page-modular | \
  python3 plot_sweep.py "hits: 105 cols x 226 rg" > results/projection_sweep.svg
```

The DBP throughput bridge calls the production SIMD decoder in `runtime/dbl/parquet` (`ReadHeader`,
`ReadBlockHeader`, and `DecodeMiniblock32`). The benchmark does not contain a separate DBP decoder.

The `dbp_throughput` tool accepts current page-first footers. Its timed loop uses a preallocated
output buffer and includes DBP page and block header parsing, but excludes file reads, Compact
Thrift parsing, and allocation.

```sh
cmake --build build -j --target dbp_throughput
build/dbp_throughput input.page-modular
```

Across the five measured maps, median throughput over three runs is 1.57-1.77 billion offsets/s,
equivalent to 12.5-14.2 GB/s of decoded `int64_t` output. Encoded-input throughput is 2.58-4.38 GB/s
because the maps have different packed bit widths. See
`results/dbp-region-map-throughput.csv`.

### Page-first result

The proposal's shape is a flat table with 2,000 columns and 10 row groups. The page-first
comparison uses a real Parquet fixture with that shape so the converter can scan actual pages. Each
operation is calibrated to roughly 200 ms, all files are already in memory, and every resolver is
fidelity-checked before timing. Results below are microseconds per operation on an Intel Xeon 6975P:

| layout | project 1 (µs/op) | project all 2,000 (µs/op) |
|---|---:|---:|
| standard Thrift materialization | 30,053.092 | 30,682.719 |
| footer walk | 2,003.583 | 1,981.928 |
| jump table | 1.936 | 1,639.144 |
| current modular | 1.499 | 571.409 |
| page-first, cold map | 47.699 | 688.146 |
| page-first, retained map | 1.525 | 641.414 |

This reproduces the proposal's projection scaling for the existing layouts. Page-first decode is
essentially tied with current modular for one projected column after the map is retained. With the
runtime SIMD decoder, the mandatory `DELTA_BINARY_PACKED` region-map decode adds roughly 46 µs/op on
this fixture. The warm path is slower at wide projection because it visits every projected page to
reconstruct the legacy chunk-shaped benchmark result, although each page field is directly
addressable. See `results/wide-2000x10-placement.csv`.

hits (105 columns × 226 row groups = 23,730 chunks), project 1, µs/op:

```
resolve      projected_us/op      all_us/op   vs_walk_x
standard        56476.407      58460.343           0.1x
walk             3071.948       3346.048           1.0x
index              27.813       3051.793         110.4x
modular            33.028       3372.619          93.0x
```

- `standard` is ~16-18x slower than `walk` — the parser-quality rung (matches the
  `hardwood` / Will's footer-parsing reference), projection-independent.
- `walk` visits all chunks, so it is ~100x a seek even at 1 column.
- `index` and `modular` are O(projected) and close (both zero-copy); `index` is
  marginally ahead because modular decodes a present-index to locate each min/max.
  Placement-only, modular is fastest and ~7x smaller (299 KB vs 2.14 MB).
- Narrow footers (e.g. yellow_tripdata, 19 columns) sit within a few µs/op across all lean
  readers — fixed overhead dominates, so they are not where decoding matters.

Corpus sweeps ([`run_corpus.py`](run_corpus.py), from `real-footer-size/corpus.json`):
[projection_sweep](results/projection_sweep.svg) (hits) ·
[us-accidents](results/us-accidents-00004-of-00007-placement.svg) ·
[fineweb](results/fineweb-10bt-000-placement.svg) ·
[hacker-news](results/hacker-news-00000-of-00039-placement.svg) ·
[yellow-taxi](results/yellow-tripdata-2025-01-placement.svg).
Checked-in numbers are one machine's — read them as trends, not absolute latency.

One projected column on the Raincloud corpus, in microseconds per operation:

| dataset | current Thrift (µs/op) | current modular (µs/op) | page cold (µs/op) | page retained (µs/op) |
|---|---:|---:|---:|---:|
| US Accidents | 78,051.480 | 27.024 | 129.727 | 25.973 |
| FineWeb 10BT | 11,702.438 | 26.302 | 38.121 | 23.934 |
| Hacker News | 21,828.798 | 26.860 | 56.582 | 24.423 |
| Yellow Taxi | 101.753 | 0.129 | 0.459 | 0.179 |

Statistics are intentionally excluded to isolate placement decode. `page cold` decodes every DBP
region offset; `page retained` reuses the resulting absolute offsets.
The page-first resolver uses legacy row-group boundaries only to construct the same benchmark
result as the current layouts; they are not part of page-first metadata. Codec DBP decoding, page
headers, and file I/O are also outside this workload.

`Current Thrift + OffsetIndex` is not a separate resolver in this chunk-shaped workload. The index
is stored outside the footer and does not avoid parsing current Thrift metadata; it is retained as a
size comparison for the page-level placement information it adds.

## 2. Name-resolve bench

Resolve query column names to ordinals two ways on a footer with a persisted name
hash (jump-table `SchemaLayout.NameHashTable`, or a modular `SCHEMA_INDEX` module via
`parquet_to_modular --schema-index`; auto-detected):

- `walk` — parse the whole schema, build name -> ordinal, look up K. O(all columns).
- `hash` — FNV-1a-64 probe per name, confirm by reconstructing its full path
  (per-element offsets + parent chain for nested schemas). O(projected).

Both return identical ordinals (cross-checked). Files: `name_resolve_bench.cc`,
`make_wide_footer.cc` (wide fixture generator), `plot_name_resolve.py`.
`../modular-footer/schema_index_check.cc` verifies every leaf resolves (flat + nested).

```sh
cmake --build build -j --target make_wide_footer modular_footer_convert name_resolve_bench
../build/make_wide_footer --columns 5000 --row-groups 4 wide.parquet
../build/modular_footer_convert --schema-index wide.parquet wide.si.modular
../build/name_resolve_bench wide.si.modular --sweep | \
  python3 plot_name_resolve.py "queried names (of 5000)" "Name resolution" > results/name_resolve.svg
```

Resolve 1 name; walk grows O(columns), hash stays flat:

| columns (count) | walk (µs/op) | hash (µs/op) | speedup (×) |
|--------:|------:|------:|--------:|
| 500 | 44 | 0.09 | ~475 |
| 5,000 | 439 | 0.09 | ~4,800 |
| 20,000 | 1,976 | 0.13 | ~15,000 |

The hash rises with K and only crosses over near full projection, so it wins exactly
in the common case: a selective projection of a wide schema. The `SCHEMA_INDEX` module
costs ~3% of a lean footer at 5,000 columns and is written only on demand
(see `../modular-footer/`). Sweeps:
[width](results/name_resolve_width_sweep.svg) ·
[projection](results/name_resolve_wide5000_ksweep.svg).

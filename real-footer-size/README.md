# Real Parquet footer-size benchmark

This benchmark compares footer representations using four unmodified Parquet files curated by
[Raincloud](https://github.com/spiraldb/raincloud). Three naturally have large footers; Yellow
Taxi is the normal-footer control. URLs, object sizes, footer sizes, and SHA-256 hashes are pinned
in `corpus.json`.

The tools are deliberately separate:

```sh
# Fetch only eight bytes per object and verify the pinned remote sizes.
python3 real-footer-size/probe_remote.py

# Download all four files (~2.65 GB) and verify their complete SHA-256 hashes.
python3 real-footer-size/download.py

# Inspect exact standard footer sizes from the downloaded files.
python3 real-footer-size/footer_size.py

# Regenerate the checked-in visualization.
python3 real-footer-size/visualize.py

# Regenerate the separate stacked component visualization.
python3 real-footer-size/footer_breakdown.py
```

Build the dependency-free modular converter before running the footer measurement or either
visualization command:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j --target modular_footer_convert
```

To produce a complete file that the experimental Hardwood reader can query, replace only the
standard footer while preserving the original header, data pages, and page-index bytes:

```sh
./build/modular_footer_convert --full-file --truncate-minmax=16 \
  real-footer-size/data/yellow-tripdata-2025-01.parquet \
  real-footer-size/data/yellow-tripdata-2025-01.modular.parquet
```

Complete modular files end in
`[modular_start: LE i64][root_offset: LE i64][MFP1]`. Both the root offset and module offsets are
relative to `modular_start`. Without `--full-file`, the converter continues to emit the existing
metadata-only `MFT1` file.

![Normalized footer-size comparison](footer-size.svg)

![Footer component breakdown](footer-components.svg)

Downloads use an atomic `.part` file and land in the gitignored `real-footer-size/data/` directory.
Pass one or more dataset names to either command to operate on a subset.

`footer_size.py` owns footer inspection and comparison; the downloader contains no footer-format
logic. It reports three compact-Thrift representations:

1. The byte-for-byte standard Parquet footer.
2. The standard footer with `ColumnMetaData.path_in_schema` omitted. This is experimental because
   the field is required by the current Parquet Thrift definition.
3. Variant 2 with each column chunk's statistics represented by one common prefix for min and max,
   followed by separate min and max suffixes capped at 16 bytes. Deprecated duplicate min/max
   fields are removed, and existing exactness flags are cleared when either suffix is truncated.

The third representation uses statistics field IDs 1, 2, and 5 for the prefix, min suffix, and max
suffix respectively. It is an explicit experimental wire layout, not standard Parquet. Change the
limit with `--suffix-limit`.

Before transforming a footer, the tool decodes and re-encodes it and requires byte-for-byte
equality. Thus every standard size is fidelity-checked against the actual downloaded footer rather
than estimated.

Current results with a 16-byte suffix limit:

| dataset | standard (bytes) | no path (bytes) | prefix + suffix16 (bytes) | modular (bytes) |
|---|---:|---:|---:|---:|
| US Accidents | 4,747,144 | 4,080,829 | 3,616,584 | 1,765,233 |
| FineWeb 10BT | 3,971,883 | 3,881,669 | 1,408,067 | 531,503 |
| Hacker News | 1,840,138 | 1,698,502 | 1,183,709 | 598,845 |
| Yellow Taxi | 11,212 | 9,900 | 8,512 | 5,879 |

The comparison excludes page indexes. Standard Parquet stores its page-index blobs outside the
footer, while the modular representation embeds them in modules, so including them on only one
side would not be an apples-to-apples footer-size comparison.

## Page-first modular footer

The [design notes](../modular-footer/PageFirstModularFooter.md) and
[`PageFirstModularFooter.thrift`](../modular-footer/PageFirstModularFooter.thrift) replace
column-chunk placement with two layers:
an always-read array of absolute region offsets and independently addressed logical page
descriptors for each column. Every complete serialized page is exactly one region. Region offsets
use strict Parquet `DELTA_BINARY_PACKED`. Each column
descriptor stores one data-page count, one whole-column dictionary-encoding boolean, five packed
fixed-width random-access binaries, and one Parquet `DELTA_BINARY_PACKED` codec stream. A constant
codec stream uses one block and one miniblock covering the complete column. Exact encodings remain
in page headers. Dictionary pages remain physical regions and are reached through direct data-page
references; they do not occupy logical descriptor rows. Codecs use DBP; dictionary references are
not coalesced. A schema-present column with zero data pages represents an all-null column.

The converter scans actual `PageHeader` values because the legacy footer does not contain a page
inventory. DataPageV2 carries its top-level row count directly. For nested DataPageV1 input, the
converter decodes repetition levels; uncompressed, Snappy, and GZIP pages are currently supported.

```sh
python3 modular-footer/parquet_to_page_modular.py \
  real-footer-size/data/yellow-tripdata-2025-01.parquet \
  /tmp/yellow.page-modular

python3 real-footer-size/page_footer_size.py
```

The comparison script reports:

1. the current byte-for-byte PAR1 footer;
2. a synthetic current-format `OffsetIndex` for every column chunk plus the resulting footer
   locator fields;
3. the current modular proposal's column-chunk `placement` module;
4. the complete current modular footer;
5. page-first placement, comprising the DBP region map, fixed-width per-page arrays, and DBP
   codecs;
   and
6. the complete page-first modular footer with schema, page metadata, converted row-range
   statistics with DBP start rows, file metadata, and root.

`--full-file` preserves the original data prefix and replaces the PAR1 footer with the experimental
metadata and a `[region map location][root location][PMF1]` trailer. Without it, the converter emits
only the metadata bytes and trailer for size analysis.

Current page-first results:

The machine-readable output is in `page-footer-size.csv`.

| dataset | chunks (count) | data pages (count) | dictionary pages (count) | total pages (count) | current footer (bytes) | current + offset indexes (bytes) | column-chunk placement (bytes) | page placement (bytes) | placement change | complete column-chunk footer (bytes) | complete page footer (bytes) | complete change |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| US Accidents | 50,830 | 50,830 | 36,465 | 87,295 | 4,747,144 | 5,872,211 | 694,618 | 686,667 | −1.1% | 1,764,414 | 1,759,451 | −0.3% |
| FineWeb 10BT | 9,441 | 9,441 | 9,441 | 18,882 | 3,971,883 | 4,187,395 | 158,380 | 133,976 | −15.4% | 530,722 | 509,561 | −4.0% |
| Hacker News | 15,022 | 15,022 | 12,876 | 27,898 | 1,840,138 | 2,177,310 | 223,029 | 227,528 | +2.0% | 598,045 | 603,440 | +0.9% |
| Yellow Taxi | 80 | 145 | 80 | 225 | 11,212 | 13,868 | 1,404 | 2,927 | +108.5% | 5,877 | 7,753 | +31.9% |

Row-range statistics account for the following bytes, including the per-column locator array:

| dataset | current modular row-group statistics (bytes) | page-first row-range statistics (bytes) | change |
|---|---:|---:|---:|
| US Accidents | 1,059,755 | 1,062,628 | +0.3% |
| FineWeb 10BT | 371,353 | 374,588 | +0.9% |
| Hacker News | 371,678 | 372,551 | +0.2% |
| Yellow Taxi | 2,265 | 2,575 | +13.7% |

Column-chunk placement is the current modular proposal's serialized `placement` module alone; its
complete footer includes all modules emitted by Jiayi's converter. Page placement comprises the
DBP region map, page directory, one dictionary-encoding boolean, five aligned fixed-width binaries,
and one DBP codec stream sharing one data-page count per column. Dictionary pages still have region
boundaries, but are represented logically only by direct references from data pages.
Physical types remain in the shared schema and `num_rows` remains in the shared root, so neither
whole structure is charged to page placement. The complete page footer additionally includes
schema, converted row-range statistics, file metadata, and the root. Current + offset indexes
includes both serialized Thrift `OffsetIndex` blobs and the footer growth from adding their
per-chunk locators.

On the three large files, dense DBP range starts put page-first statistics within 1% of current
modular statistics and complete page-first footers range from 4% smaller to 1% larger than current
modular.

Both prefix-based representations cap each min/max suffix at the configured limit (16 bytes in the
table), so their statistics policies are directly comparable.

## Component breakdown

`footer-components.svg` keeps the total-size graph separate and divides both representations into
`path_in_schema`, row-group statistics, schema, placement, key/value metadata, and residual
framing. Placement covers offsets, sizes, codecs, and physical types. The OSS split uses
progressive clearing: it removes every path, every `ColumnMetaData.statistics`, then
`FileMetaData.row_groups`, `FileMetaData.schema`, and `FileMetaData.key_value_metadata`. The
serialized-size difference at each step is assigned to that component, so the components add up
exactly despite compact-Thrift field-header interactions.

For modular footers, schema and placement are the corresponding modules and `path_in_schema` is
zero because the schema supplies the mapping. Statistics include both the per-column descriptors
and the row-group-statistics directory. Key/value metadata is isolated inside the file-metadata
module; created-by, the modular root directory, and framing remain in other. Page indexes are
excluded from both representations.

## Common-prefix sharing versus truncation

The last column above combines two effects. Running with an effectively unlimited suffix isolates
the common-prefix representation; comparing that result with the 16-byte result isolates the
additional effect of truncation:

| dataset | no path (bytes) | prefix, untruncated (bytes) | prefix + suffix16 (bytes) | prefix saving (bytes) | truncation saving (bytes) |
|---|---:|---:|---:|---:|---:|
| US Accidents | 4,080,829 | 3,744,296 | 3,616,584 | 336,533 | 127,712 |
| FineWeb 10BT | 3,881,669 | 3,784,175 | 1,408,067 | 97,494 | 2,376,108 |
| Hacker News | 1,698,502 | 1,563,106 | 1,183,709 | 135,396 | 379,397 |
| Yellow Taxi | 9,900 | 8,512 | 8,512 | 1,388 | 0 |

FineWeb's large reduction is therefore almost entirely truncation (96% of its statistics saving),
while US Accidents benefits mostly from the prefix representation. Hacker News is mixed but leans
toward truncation, and Yellow Taxi has no statistic longer than the 16-byte suffix limit.

There is one important accounting caveat: the standard Arrow-written files contain both legacy
`min`/`max` and modern `min_value`/`max_value` fields. The experimental representation replaces
those four bounds with one prefix and two suffixes. Consequently, "prefix saving" above includes
both common-prefix sharing and removal of the duplicate legacy bounds; it does not measure prefix
sharing in isolation.

Reproduce the two measurements with:

```sh
python3 real-footer-size/footer_size.py --suffix-limit 1000000000 --csv
python3 real-footer-size/footer_size.py --suffix-limit 16 --csv
```

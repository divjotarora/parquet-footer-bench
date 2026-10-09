# Page-First Modular Footer Metadata

**Status: design sketch**

This sketch supersedes the earlier encoding-domain prototype. It adapts the modular footer to the
page-first data model by separating physical byte addressing from logical page metadata. Most core
per-page metadata is represented by aligned, dense, random-access arrays. The global region offset
array and each projected column's codec stream use delta binary packing.

## Design principles

- Every serialized Parquet page, including its `PageHeader` and body, is exactly one physical
  region. A region never contains more than one page.
- Other independently addressable objects, including metadata modules, extensions, padding, and
  stashed compatibility data, may also be regions.
- The region map contains physical boundaries only. It does not assign columns, rows, page kinds,
  encodings, or other semantics to a region.
- All structures other than the fixed bootstrap trailer and root refer to file bytes by region
  ordinal rather than repeating byte offsets and lengths.
- Page ownership, logical order, row coverage, compression, and dictionary dependencies live in
  independently addressable column metadata.

## Regions

The region map is an ordered array of absolute file offsets:

```text
region r = [region_offsets[r], region_offsets[r + 1])
```

For `R` regions, `region_offsets` contains `R + 1` strictly increasing values. Absolute offsets use
Parquet's standard `DELTA_BINARY_PACKED` encoding with 128-value blocks and four 32-value
miniblocks. Typical page and module sizes therefore become small deltas. The encoded stream is
sequential; decoded entries are absolute boundaries.

Every page boundary must appear in the array. An unreferenced region represents padding, a gap, or
an object unknown to the reader. Several entities may refer to the same complete region. Partial
sharing requires splitting at every referenced endpoint and representing the entity as a sequence
of regions.

Regions are metadata addressing units, not I/O request units. A reader may fetch a byte range that
covers several adjacent regions, but page identity, validation, dependencies, and decoding remain
separate.

The fixed trailer is the bootstrap exception. It locates the root and region map directly. Once the
region map is available, module-directory entries and all other locators use region ordinals.

## Required modules

```text
ModularFooter
├── Schema
├── RegionMap
├── PageDirectory
└── optional modules
    ├── ColumnRangeStatistics
    ├── BloomFilters
    ├── FileMetadata
    └── extensions
```

The root retains `version`, `num_columns`, `num_rows`, and the module directory. It no longer
contains `num_row_groups`.

### Region map

```thrift
struct RegionMap {
  /** UINT64 absolute offsets; num_regions + 1 strictly increasing entries. */
  1: required RegionOffsetArray region_offsets
}
```

`RegionOffsetArray` uses strict Parquet `DELTA_BINARY_PACKED`. This is the only always-read
sequential array in the core design. Readers decode and retain it before resolving region ordinals.

### Page directory

The page directory locates an independently serialized page descriptor for each leaf column.
Readers can therefore load page metadata in proportion to projection rather than decoding every
column's logical page arrays.

Each column descriptor contains one shared data-page count, one dictionary-encoding summary, five
aligned fixed-width binaries, and one DBP codec stream:

```text
is_fully_dictionary_encoded
region_ordinals[]
first_row_indexes[]
codecs[]
dictionary_regions[]
num_values[]
total_uncompressed_sizes[]
```

Entries are in logical data-page order. Row indexes are absolute, zero-based top-level file row
indexes. `dictionary_regions` stores the directly referenced dictionary region plus one, with zero
meaning no dictionary. Dictionary pages remain physical regions but do not occupy descriptor rows.
A dictionary is compressed with the codec of the referring data page; all references to the same
dictionary must therefore agree on codec. Dictionary encoding, value count, and uncompressed size
remain available in its `PageHeader`.

Every binary except `codecs` is `[u8 bit_width][packed values]` with O(1) positional access. The
count is stored once rather than repeating generic `EncodedArray` framing for every field. Codecs
use Parquet `DELTA_BINARY_PACKED`. A constant codec sequence uses one block and one miniblock sized
to cover the complete column, so all deltas have bit width zero. Nonconstant sequences use
128-value blocks with four 32-value miniblocks. There is no RLE, sparse presence, default value,
domain table, or other coalescing. Value count and uncompressed size remain physically repeated
once per data page. Physical region ordinals need not be monotonic.

`is_fully_dictionary_encoded` summarizes the complete column rather than each column chunk. Exact
per-page encodings remain in page headers; direct dictionary-region references identify the pages
that use a dictionary. Physical type remains in the schema. Total compressed bytes are derived from
region boundaries; value and uncompressed-byte totals are derived from the per-page arrays.

A schema-present column with `num_data_pages == 0` represents an all-null column over the root's
`num_rows`; it is not a missing column. Its packed arrays are empty and
`is_fully_dictionary_encoded` is false. This convention permits writers to omit physical pages for
flat all-null columns. A nested or repeated leaf may be omitted only when its repetition and
definition structure can be reconstructed without that leaf's pages; otherwise it still requires
physical pages.

## Optional metadata

### Column-range statistics

The current row-group statistics module is replaced by independently selected per-column row-range
statistics. Ranges use the same absolute top-level row coordinate as page metadata. Statistics
boundaries do not close pages or dictionaries.

Each leaf column has one independently addressable descriptor. The column is implicit in the
column-indexed locator array; a column without statistics has no descriptor entry. A participating
column must cover every row in the file with a dense partition of statistics blocks. Its descriptor
stores only absolute `range_start_rows`, beginning with zero and strictly increasing. Block `i` ends
at start `i + 1`; the final block ends at the root's `num_rows`.

For example, a 10,000-row column partitioned at rows 1,000, 3,000, and 5,000 stores:

```text
range_start_rows = [0, 1000, 3000, 5000]
```

This describes `[0, 1000)`, `[1000, 3000)`, `[3000, 5000)`, and `[5000, 10000)`.
`range_start_rows` uses Parquet `DELTA_BINARY_PACKED`, so locating a column descriptor is O(1) but
decoding ranges within that column is sequential. Aligned optional arrays hold null counts, min/max
values, exactness, NaN counts, and future statistics using the current modular `EncodedArray`
sparsity. Every block must publish at least one statistic. Each column chooses its own dense
partition, without a shared granularity, preamble, or row-group identity.

There is no separate page-level `ColumnIndex` module. A writer that wants page-granular pruning
publishes row-range blocks that tile its pages. Placement maps matching row ranges to pages using
absolute `first_row_indexes`. This keeps one statistics model for both legacy row-group files and
page-first layouts.

### Other modules

Bloom filters, file metadata, schema indexes, compatibility metadata, and future extensions are
independently serialized regions. Their directory entries contain region ordinals rather than
absolute offset/length pairs.

## Replacement of current modular placement

| Current `PlacementModule` field | Page-first representation |
| --- | --- |
| `data_page_offsets` | Data-page region ordinals plus `RegionMap` |
| `first_dictionary_pages` | Direct per-page dictionary-region references |
| `dictionary_page_offsets` | Direct dictionary region ordinals plus `RegionMap` |
| `total_compressed_sizes` | Derived from referenced region boundaries |
| `total_uncompressed_sizes` | Sum of per-page uncompressed sizes |
| `num_values` | Sum of per-page value counts for data pages |
| `codecs` | Per-column DBP codec stream |
| `physical_types` | Schema |
| `is_fully_dictionary_encoded` | One boolean for the complete column |
| `row_group_num_rows` | Removed; pages use absolute file row indexes |

The optional `OFFSET_INDEX` module also disappears. The region map supplies page byte ranges, while
the page directory supplies logical order and first-row indexes.

## Compatibility

This design requires a new footer/version boundary. The legacy footer cannot express independent
page placement, absolute page row coordinates, or multiple dictionaries. A legacy footer may
be retained as an opaque region during a transition, but it is authoritative only when the physical
layout is representable by the legacy row-group and column-chunk model.

Encryption AADs must identify pages by stable logical identity, such as `(column ordinal, logical
page ordinal)`, rather than by row-group ordinal or physical region ordinal.

## Open questions

- Whether `region_offsets` needs a separate bounded-random-access index; standard Parquet
  `DELTA_BINARY_PACKED` is sequential.
- Whether the complete region map always fits the speculative-tail budget or needs block-level
  framing while remaining mandatory.
- Representation of data pages that begin in the same repeated top-level row.
- Whether fixed-width absolute first-row and region arrays remain acceptable at much larger scale,
  or need a separately indexed random-access encoding.
- Whether optional column summaries are required for resource planning in addition to per-page
  values.
- Page and module encryption identities, integrity digests, and unknown-module preservation.
- Representation of sorting guarantees after removing row groups.

## Flat-array Raincloud size results

The prototype in the local `parquet-footer-bench` clone scans every real page header in the four
files used by the modular-footer proposal. It synthesizes a current-format Thrift `OffsetIndex` for
every column chunk, adds the corresponding offset/length fields to the current footer, and converts
the same file to the page-first modules above.

| Dataset | Column chunks (count) | Data pages (count) | Dictionary pages (count) | Total pages (count) | Current footer (bytes) | Current footer + offset indexes (bytes) | Column-chunk placement (bytes) | Page placement (bytes) | Placement change | Complete column-chunk footer (bytes) | Complete page footer (bytes) | Complete change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| US Accidents | 50,830 | 50,830 | 36,465 | 87,295 | 4,747,144 | 5,872,211 | 694,618 | 686,667 | −1.1% | 1,764,414 | 1,759,451 | −0.3% |
| FineWeb 10BT | 9,441 | 9,441 | 9,441 | 18,882 | 3,971,883 | 4,187,395 | 158,380 | 133,976 | −15.4% | 530,722 | 509,561 | −4.0% |
| Hacker News | 15,022 | 15,022 | 12,876 | 27,898 | 1,840,138 | 2,177,310 | 223,029 | 227,528 | +2.0% | 598,045 | 603,440 | +0.9% |
| Yellow Taxi | 80 | 145 | 80 | 225 | 11,212 | 13,868 | 1,404 | 2,927 | +108.5% | 5,877 | 7,753 | +31.9% |

| Dataset | Current modular row-group statistics (bytes) | Page-first row-range statistics (bytes) | Change |
| --- | ---: | ---: | ---: |
| US Accidents | 1,059,755 | 1,062,628 | +0.3% |
| FineWeb 10BT | 371,353 | 374,588 | +0.9% |
| Hacker News | 371,678 | 372,551 | +0.2% |
| Yellow Taxi | 2,265 | 2,575 | +13.7% |

`Current footer + offset indexes` includes the serialized index blobs and the growth of the Thrift
footer from adding each chunk's index offset and length. The index blobs alone are 718,427 bytes,
139,984 bytes, 216,996 bytes, and 2,090 bytes respectively.

`Column-chunk placement` is the current modular proposal's serialized `placement` module alone;
`Complete column-chunk footer` includes all modules emitted by Jiayi's converter. `Page placement`
includes the DBP region-offset map, page directory, one per-column dictionary-encoding
boolean, five fixed-width per-data-page arrays, and one DBP codec stream. Physical types remain in
the shared schema and `num_rows` remains in the shared root, so neither whole structure is charged
to page placement. `Complete page footer` additionally includes schema, converted row-range
statistics, file metadata, and the root. It covers the same module categories used in the current
modular-footer comparison.

The three large files have exactly one data page per column chunk. A Thrift offset index adds 211
KiB to 1.07 MiB after locator growth. Describing only logical data pages makes page placement 15%
smaller to 2% larger than column-chunk placement on those files. Dense DBP range starts bring
page-first statistics to within 1% of current modular on the three large files, and their complete
page-first footers range from 4% smaller to 1% larger than current modular.

These measurements use the legacy physical layout. Except for codecs, the per-column arrays remain
fixed-width absolute values even where the values happen to be monotonic. Codec savings depend on
repetition; no other measured saving depends on runs, defaults, or shared dictionaries.

## Flat-array decode results

The C++ footer-decode harness has a fidelity-checked page-first resolver. `Page cold` decodes the
mandatory DBP region offsets on each operation; `Page warm` retains the absolute offsets and
measures projected column descriptors. Inputs are already in
memory; each case runs for roughly 200 ms. Absolute timings are specific to an Intel Xeon 6975P.

At a one-column projection, the placement-only Raincloud workload is:

| Dataset | Current Thrift (µs/op) | Current modular (µs/op) | Page cold (µs/op) | Page warm (µs/op) |
| --- | ---: | ---: | ---: | ---: |
| US Accidents | 78,051.480 | 27.024 | 129.727 | 25.973 |
| FineWeb 10BT | 11,702.438 | 26.302 | 38.121 | 23.934 |
| Hacker News | 21,828.798 | 26.860 | 56.582 | 24.423 |
| Yellow Taxi | 101.753 | 0.129 | 0.459 | 0.179 |

Statistics are intentionally excluded from the page-first timing comparison. The row-range layout
above is included in footer-size accounting, but its decode workload has not been implemented or
measured.

`Current Thrift` fully materializes the current Thrift footer into an object tree. `Page cold`
includes the mandatory DBP region-offset decode; `Page warm` retains the resulting offsets. The
flat page arrays are accessed directly without decoding preceding entries. In the
placement-only workload, retained-map page-first takes 0.91-0.96x current modular time on the three
large files and 1.39x on Yellow Taxi. These results do not include I/O, page-header decoding,
codec-stream decoding, or statistics. `Current Thrift + OffsetIndex` is a size comparison, not a
separate resolver here: the index is stored outside the footer and this workload still requires
parsing current Thrift metadata.

/**
 * Page-first modular footer experiment.
 *
 * The fixed trailer locates RegionMap and PageFirstModularFooter directly. Every other
 * independently addressable object is exactly one region and is located by region ordinal.
 */

include "ModularFooter.thrift"

namespace cpp parquet.modular.page
namespace java org.apache.parquet.format.modular.page

/**
 * The one sequential array in the page-first footer. values uses DELTA_BINARY_PACKED from the
 * Parquet encodings specification verbatim, with 128-value blocks and four 32-value miniblocks.
 */
enum RegionOffsetEncoding {
  DELTA_BINARY_PACKED = 2
}

struct RegionOffsetArray {
  1: required i32 num_values,
  2: required binary values,
  3: required RegionOffsetEncoding encoding
}

/**
 * Physical byte boundaries only. For R regions, region_offsets has R + 1 strictly increasing
 * absolute file offsets and region r is [region_offsets[r], region_offsets[r + 1]). Every complete
 * serialized Parquet page is exactly one region.
 */
struct RegionMap {
  1: required RegionOffsetArray region_offsets
}

/**
 * Except codecs, every packed binary is [u8 bit_width][num_data_pages values, bit_width bits each,
 * LSB-first] and supports O(1) access by column-local data-page ordinal. Codecs use Parquet
 * DELTA_BINARY_PACKED. No field uses EncodedArray framing, RLE, sparse presence, defaults, or
 * shared-domain tables. Dictionary pages remain physical regions and are referenced directly by
 * data pages.
 */
struct ColumnDataPages {
  /** Number of aligned data-page positions. Zero means the complete column is null. */
  1: required i32 num_data_pages,
  /** Packed UINT64 region ordinals containing each complete PageHeader and body. */
  2: required binary region_ordinals,
  /** Packed UINT64 absolute, zero-based top-level first rows. */
  3: required binary first_row_indexes,
  /** True when every data page references a dictionary page; false when num_data_pages is zero. */
  4: required bool is_fully_dictionary_encoded,
  /** Parquet DELTA_BINARY_PACKED CompressionCodec values. */
  5: required binary codecs,
  /** Packed UINT64 dictionary region + 1; zero when unused. The dictionary uses this codec. */
  6: required binary dictionary_regions,
  /** Packed UINT64 page value counts. */
  7: required binary num_values,
  /** Packed UINT64 uncompressed bytes including PageHeader and body. */
  8: required binary total_uncompressed_sizes
}

/** One region ordinal per leaf column, pointing to its independently serialized ColumnDataPages. */
struct PageDirectory {
  1: required ModularFooter.EncodedArray column_data_page_regions
}

/**
 * Per-column statistics over a dense partition of all top-level rows. range_start_rows is a
 * Parquet DELTA_BINARY_PACKED stream. Block i ends at start i + 1, or file num_rows for the final
 * block. Statistic arrays use the same domain but may be sparse by statistic type.
 */
struct ColumnRangeStatistics {
  /** DBP UINT64 absolute top-level starting rows; starts at zero and is strictly increasing. */
  1: required binary range_start_rows,
  // Field 2 was range_num_rows; block ends are now derived.
  3: optional ModularFooter.EncodedArray null_counts,
  4: optional ModularFooter.EncodedArray minmax_prefixes,
  5: optional ModularFooter.EncodedArray min_suffixes,
  6: optional ModularFooter.EncodedArray max_suffixes,
  7: optional ModularFooter.EncodedArray min_is_exact,
  8: optional ModularFooter.EncodedArray max_is_exact,
  9: optional ModularFooter.EncodedArray nan_counts
}

/** One region ordinal per participating leaf column, pointing to its dense row partition. */
struct ColumnRangeStatisticsDirectory {
  /** UINT64 region ordinal for each column descriptor; absent positions have no statistics. */
  1: required ModularFooter.EncodedArray column_statistics_regions
}

enum ModuleKind {
  SCHEMA = 0,
  PAGE_DIRECTORY = 1,
  // Values 2, 3, and 5 are reserved from earlier experiments.
  COLUMN_RANGE_STATISTICS = 4,
  FILE_METADATA = 6,
  SCHEMA_INDEX = 7
}

/** One module occupies one complete region. */
struct ModuleDirectoryEntry {
  1: required ModuleKind kind,
  2: required i64 region_ordinal
}

/** Always-read logical root. RegionMap is located independently by the fixed trailer. */
struct PageFirstModularFooter {
  1: required i32 version,
  2: required i32 num_columns,
  3: required i64 num_rows,
  4: required list<ModuleDirectoryEntry> modules
}

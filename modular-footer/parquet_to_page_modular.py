#!/usr/bin/env python3
"""Convert a complete PAR1 file to the experimental page-first modular footer."""

import argparse
import bisect
import copy
import gzip
import os
import struct
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "real-footer-size"))
import compact  # noqa: E402


ENC_BITSET = 0
ENC_PRESENT_INDEX = 1
ENC_DELTA_BINARY_PACKED = 2

K_SCHEMA = 0
K_PAGE_DIRECTORY = 1
K_COLUMN_RANGE_STATISTICS = 4
K_FILE_METADATA = 6

DATA_PAGE = 0
DICTIONARY_PAGE = 2
DATA_PAGE_V2 = 3
PLAIN_DICTIONARY = 2
RLE_DICTIONARY = 8


def field(fields, field_id):
    for item in fields:
        if item[0] == field_id:
            return item
    return None


def required(fields, field_id, value_type, name):
    item = field(fields, field_id)
    if item is None or item[1] != value_type:
        raise ValueError("{} is missing".format(name))
    return item[2]


def optional_int(fields, field_id, default=None):
    item = field(fields, field_id)
    return default if item is None else item[2]


def uleb(value):
    output = bytearray()
    while value >= 0x80:
        output.append((value & 0x7f) | 0x80)
        value >>= 7
    output.append(value)
    return bytes(output)


def zigzag(value):
    return uleb((value << 1) ^ (value >> 63))


def pack_bits(values, width):
    output = bytearray((len(values) * width + 7) // 8)
    bit_offset = 0
    for value in values:
        for bit in range(width):
            if value & (1 << bit):
                absolute = bit_offset + bit
                output[absolute // 8] |= 1 << (absolute % 8)
        bit_offset += width
    return bytes(output)


def bit_width(values):
    return max(values, default=0).bit_length()


def dense_values(values):
    width = bit_width(values)
    return bytes([width]) + pack_bits(values, width)


def delta_values(values):
    constant = not values or min(values) == max(values)
    delta_count = max(1, len(values) - 1)
    block_size = ((delta_count + 127) // 128) * 128 if constant else 128
    mini_block_count = 1 if constant else 4
    mini_block_size = block_size // mini_block_count
    deltas = [values[index] - values[index - 1] for index in range(1, len(values))]
    output = bytearray()
    output.extend(uleb(block_size))
    output.extend(uleb(mini_block_count))
    output.extend(uleb(len(values)))
    output.extend(zigzag(values[0] if values else 0))
    for block_start in range(0, len(deltas), block_size):
        block = deltas[block_start:block_start + block_size]
        minimum = min(block)
        output.extend(zigzag(minimum))
        widths = []
        mini_blocks = []
        for mini_start in range(0, block_size, mini_block_size):
            mini_block = block[mini_start:mini_start + mini_block_size]
            residuals = [value - minimum for value in mini_block]
            width = bit_width(residuals)
            widths.append(width)
            mini_blocks.append(residuals + [0] * (mini_block_size - len(residuals)))
        output.extend(bytes(widths))
        for residuals, width in zip(mini_blocks, widths):
            output.extend(pack_bits(residuals, width))
    return bytes(output)


def delta_array(values):
    return [
        [1, compact.I32, len(values)],
        [2, compact.BINARY, delta_values(values)],
        [3, compact.I32, ENC_DELTA_BINARY_PACKED],
    ]


def dense_array(values):
    return [
        [1, compact.I32, len(values)],
        [2, compact.BINARY, dense_values(values)],
    ]


def sparse_int_array(domain, positions, values):
    value_width = bit_width(values)
    value_payload = bytes([value_width]) + pack_bits(values, value_width)
    position_width = bit_width(positions)
    presence = (
        uleb(len(positions))
        + bytes([position_width])
        + pack_bits(positions, position_width)
    )
    return [
        [1, compact.I32, domain],
        [2, compact.BINARY, value_payload],
        [3, compact.I32, ENC_PRESENT_INDEX],
        [4, compact.BINARY, presence],
    ]


def byte_values(values):
    offsets = [0]
    data = bytearray()
    for value in values:
        data.extend(value)
        offsets.append(len(data))
    width = bit_width(offsets)
    return bytes([width]) + pack_bits(offsets, width) + bytes(data)


def sparse_bytes_array(domain, positions, values):
    position_width = bit_width(positions)
    presence = (
        uleb(len(positions))
        + bytes([position_width])
        + pack_bits(positions, position_width)
    )
    return [
        [1, compact.I32, domain],
        [2, compact.BINARY, byte_values(values)],
        [3, compact.I32, ENC_PRESENT_INDEX],
        [4, compact.BINARY, presence],
    ]


def encoded_field(field_id, array):
    return [field_id, compact.STRUCT, array]


def read_footer(path):
    file_size = path.stat().st_size
    with path.open("rb") as stream:
        if stream.read(4) != b"PAR1":
            raise ValueError("missing leading PAR1 magic")
        stream.seek(-8, os.SEEK_END)
        trailer = stream.read(8)
        if trailer[4:] != b"PAR1":
            raise ValueError("missing trailing PAR1 magic")
        footer_size = struct.unpack("<I", trailer[:4])[0]
        footer_start = file_size - 8 - footer_size
        stream.seek(footer_start)
        footer_bytes = stream.read(footer_size)
    footer = compact.decode(footer_bytes)
    if compact.encode(footer) != footer_bytes:
        raise ValueError("compact-Thrift footer fidelity check failed")
    return file_size, footer_start, footer_bytes, footer


def row_groups(footer):
    value = required(footer, 4, compact.LIST, "FileMetaData.row_groups")
    if value[0] != compact.STRUCT:
        raise ValueError("FileMetaData.row_groups has the wrong element type")
    return value[1]


def column_chunks(row_group):
    value = required(row_group, 1, compact.LIST, "RowGroup.columns")
    if value[0] != compact.STRUCT:
        raise ValueError("RowGroup.columns has the wrong element type")
    return value[1]


def column_metadata(chunk):
    return required(chunk, 3, compact.STRUCT, "ColumnChunk.meta_data")


def schema_leaf_max_repetition_levels(footer):
    schema = required(footer, 2, compact.LIST, "FileMetaData.schema")[1]
    leaves = []

    def visit(index, inherited_level):
        element = schema[index]
        repetition_type = optional_int(element, 3)
        level = inherited_level + int(repetition_type == 2)
        num_children = optional_int(element, 5)
        next_index = index + 1
        if num_children is None:
            leaves.append(level)
            return next_index
        for unused in range(num_children):
            next_index = visit(next_index, level)
        return next_index

    if visit(0, 0) != len(schema):
        raise ValueError("schema traversal did not consume every element")
    return leaves


class Page:
    def __init__(
        self, column, row_group, offset, size, page_type, encoding, first_row,
        first_row_in_group, codec, num_values, total_uncompressed_size
    ):
        self.column = column
        self.row_group = row_group
        self.offset = offset
        self.size = size
        self.page_type = page_type
        self.encoding = encoding
        self.first_row = first_row
        self.first_row_in_group = first_row_in_group
        self.codec = codec
        self.num_values = num_values
        self.total_uncompressed_size = total_uncompressed_size
        self.region = None
        self.dictionary_region = None
        self.starts_with_continuation = False


class Chunk:
    def __init__(self, column, row_group, thrift, metadata):
        self.column = column
        self.row_group = row_group
        self.thrift = thrift
        self.metadata = metadata
        self.codec = required(metadata, 4, compact.I32, "ColumnMetaData.codec")
        self.num_values = required(metadata, 5, compact.I64, "ColumnMetaData.num_values")
        self.total_uncompressed = required(
            metadata, 6, compact.I64, "ColumnMetaData.total_uncompressed_size"
        )
        self.total_compressed = required(
            metadata, 7, compact.I64, "ColumnMetaData.total_compressed_size"
        )
        self.data_offset = required(
            metadata, 9, compact.I64, "ColumnMetaData.data_page_offset"
        )
        self.dictionary_offset = optional_int(metadata, 11)
        self.pages = []


def parse_page_header(fields):
    page_type = required(fields, 1, compact.I32, "PageHeader.type")
    uncompressed_size = required(fields, 2, compact.I32, "PageHeader.uncompressed_page_size")
    compressed_size = required(fields, 3, compact.I32, "PageHeader.compressed_page_size")
    encoding = None
    num_values = None
    num_rows = None
    repetition_encoding = None
    if page_type == DATA_PAGE:
        header = required(fields, 5, compact.STRUCT, "PageHeader.data_page_header")
        num_values = required(header, 1, compact.I32, "DataPageHeader.num_values")
        encoding = required(header, 2, compact.I32, "DataPageHeader.encoding")
        repetition_encoding = required(
            header, 4, compact.I32, "DataPageHeader.repetition_level_encoding"
        )
    elif page_type == DATA_PAGE_V2:
        header = required(fields, 8, compact.STRUCT, "PageHeader.data_page_header_v2")
        num_values = required(header, 1, compact.I32, "DataPageHeaderV2.num_values")
        num_rows = required(header, 3, compact.I32, "DataPageHeaderV2.num_rows")
        encoding = required(header, 4, compact.I32, "DataPageHeaderV2.encoding")
    elif page_type == DICTIONARY_PAGE:
        header = required(fields, 7, compact.STRUCT, "PageHeader.dictionary_page_header")
        num_values = required(header, 1, compact.I32, "DictionaryPageHeader.num_values")
        encoding = required(header, 2, compact.I32, "DictionaryPageHeader.encoding")
    return (
        page_type,
        uncompressed_size,
        compressed_size,
        encoding,
        num_values,
        num_rows,
        repetition_encoding,
    )


def read_page_header(stream, offset, remaining):
    read_size = min(4096, remaining)
    while read_size:
        stream.seek(offset)
        data = stream.read(read_size)
        try:
            fields, used = compact.decode_prefix(data)
            return fields, used
        except compact.DecodeError:
            if read_size == remaining:
                raise
            read_size = min(read_size * 2, remaining)
    raise ValueError("empty page header")


def read_uleb(data, offset):
    value = 0
    shift = 0
    while shift < 70:
        if offset >= len(data):
            raise ValueError("truncated RLE/bit-packed run header")
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7f) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
    raise ValueError("invalid RLE/bit-packed run header")


def read_zigzag(data, offset):
    value, offset = read_uleb(data, offset)
    return (value >> 1) ^ -(value & 1), offset


def unpack_bits(data, width, count):
    values = []
    for index in range(count):
        value = 0
        bit_offset = index * width
        for bit in range(width):
            absolute = bit_offset + bit
            if data[absolute // 8] & (1 << (absolute % 8)):
                value |= 1 << bit
        values.append(value)
    return values


def decode_delta_array(array):
    count = required(array, 1, compact.I32, "RegionOffsetArray.num_values")
    payload = required(array, 2, compact.BINARY, "RegionOffsetArray.values")
    encoding = optional_int(array, 3, ENC_BITSET)
    if encoding != ENC_DELTA_BINARY_PACKED:
        raise ValueError("RegionOffsetArray is not delta binary packed")
    block_size, offset = read_uleb(payload, 0)
    mini_block_count, offset = read_uleb(payload, offset)
    encoded_count, offset = read_uleb(payload, offset)
    first, offset = read_zigzag(payload, offset)
    if encoded_count != count:
        raise ValueError("DELTA_BINARY_PACKED value count disagrees with RegionOffsetArray")
    if block_size % 128 or not mini_block_count or block_size % mini_block_count:
        raise ValueError("invalid DELTA_BINARY_PACKED block shape")
    mini_block_size = block_size // mini_block_count
    if mini_block_size % 32:
        raise ValueError("invalid DELTA_BINARY_PACKED miniblock size")
    values = [] if count == 0 else [first]
    while len(values) < count:
        minimum, offset = read_zigzag(payload, offset)
        if offset + mini_block_count > len(payload):
            raise ValueError("truncated DELTA_BINARY_PACKED bit widths")
        widths = payload[offset:offset + mini_block_count]
        offset += mini_block_count
        remaining = count - len(values)
        for width in widths:
            byte_count = mini_block_size * width // 8
            if offset + byte_count > len(payload):
                raise ValueError("truncated DELTA_BINARY_PACKED miniblock")
            residuals = unpack_bits(
                payload[offset:offset + byte_count], width, mini_block_size
            )
            offset += byte_count
            used = min(remaining, mini_block_size)
            for residual in residuals[:used]:
                values.append(values[-1] + minimum + residual)
            remaining -= used
    if offset != len(payload):
        raise ValueError("RegionOffsetArray has the wrong decoded length")
    return values


def unpack_dense_values(data, count):
    if not data:
        raise ValueError("packed array is empty")
    width = data[0]
    byte_count = (count * width + 7) // 8
    if width > 64 or len(data) != byte_count + 1:
        raise ValueError("packed array has the wrong length")
    return unpack_bits(data[1:], width, count)


def decode_rle_levels(data, width, count):
    if width == 0:
        return [0] * count
    values = []
    offset = 0
    while len(values) < count:
        header, offset = read_uleb(data, offset)
        if header & 1:
            run_count = (header >> 1) * 8
            byte_count = (run_count * width + 7) // 8
            if offset + byte_count > len(data):
                raise ValueError("truncated bit-packed levels")
            values.extend(unpack_bits(data[offset:offset + byte_count], width, run_count))
            offset += byte_count
        else:
            run_count = header >> 1
            byte_count = (width + 7) // 8
            if offset + byte_count > len(data):
                raise ValueError("truncated repeated level")
            value = int.from_bytes(data[offset:offset + byte_count], "little")
            values.extend([value] * run_count)
            offset += byte_count
    return values[:count]


def snappy_decompress(data, expected_size):
    declared_size, offset = read_uleb(data, 0)
    output = bytearray()
    while offset < len(data):
        tag = data[offset]
        offset += 1
        kind = tag & 3
        if kind == 0:
            length = tag >> 2
            if length < 60:
                length += 1
            else:
                length_bytes = length - 59
                if offset + length_bytes > len(data):
                    raise ValueError("truncated Snappy literal length")
                length = 1 + int.from_bytes(data[offset:offset + length_bytes], "little")
                offset += length_bytes
            if offset + length > len(data):
                raise ValueError("truncated Snappy literal")
            output.extend(data[offset:offset + length])
            offset += length
            continue
        if kind == 1:
            length = 4 + ((tag >> 2) & 7)
            if offset >= len(data):
                raise ValueError("truncated Snappy copy")
            copy_offset = ((tag & 0xe0) << 3) | data[offset]
            offset += 1
        elif kind == 2:
            length = 1 + (tag >> 2)
            if offset + 2 > len(data):
                raise ValueError("truncated Snappy copy")
            copy_offset = int.from_bytes(data[offset:offset + 2], "little")
            offset += 2
        else:
            length = 1 + (tag >> 2)
            if offset + 4 > len(data):
                raise ValueError("truncated Snappy copy")
            copy_offset = int.from_bytes(data[offset:offset + 4], "little")
            offset += 4
        if copy_offset <= 0 or copy_offset > len(output):
            raise ValueError("invalid Snappy copy offset")
        for unused in range(length):
            output.append(output[-copy_offset])
    if declared_size != len(output) or expected_size != len(output):
        raise ValueError(
            "Snappy size mismatch: declared {}, expected {}, decoded {}".format(
                declared_size, expected_size, len(output)
            )
        )
    return bytes(output)


def decompress(codec, data, uncompressed_size):
    if codec == 0:
        return data
    if codec == 1:
        return snappy_decompress(data, uncompressed_size)
    if codec == 2:
        result = gzip.decompress(data)
        if len(result) != uncompressed_size:
            raise ValueError("GZIP size mismatch")
        return result
    raise ValueError(
        "decoding nested V1 pages with compression codec {} is not implemented".format(codec)
    )


def v1_page_rows(
    stream, body_offset, compressed_size, uncompressed_size, codec, num_values, max_rep
):
    stream.seek(body_offset)
    compressed = stream.read(compressed_size)
    if len(compressed) != compressed_size:
        raise ValueError("truncated data-page body")
    body = decompress(codec, compressed, uncompressed_size)
    if len(body) < 4:
        raise ValueError("V1 data page has no repetition-level length")
    repetition_bytes = struct.unpack("<I", body[:4])[0]
    if 4 + repetition_bytes > len(body):
        raise ValueError("V1 repetition levels exceed the page body")
    levels = decode_rle_levels(body[4:4 + repetition_bytes], max_rep.bit_length(), num_values)
    if not levels:
        return 0, False
    return sum(level == 0 for level in levels), levels[0] != 0


def scan_pages(path, footer):
    groups = row_groups(footer)
    if not groups:
        raise ValueError("footer contains no row groups")
    num_columns = len(column_chunks(groups[0]))
    chunks = [[None for unused in range(num_columns)] for unused in groups]
    pages = []
    max_repetition_levels = schema_leaf_max_repetition_levels(footer)
    if len(max_repetition_levels) != num_columns:
        raise ValueError("schema leaf count does not match row-group column count")
    row_base = 0
    with path.open("rb") as stream:
        for group_ordinal, group in enumerate(groups):
            group_rows = required(group, 3, compact.I64, "RowGroup.num_rows")
            group_chunks = column_chunks(group)
            if len(group_chunks) != num_columns:
                raise ValueError("ragged row groups")
            for column, thrift_chunk in enumerate(group_chunks):
                metadata = column_metadata(thrift_chunk)
                chunk = Chunk(column, group_ordinal, thrift_chunk, metadata)
                chunks[group_ordinal][column] = chunk
                start = chunk.data_offset
                if chunk.dictionary_offset is not None:
                    start = min(start, chunk.dictionary_offset)
                end = start + chunk.total_compressed
                offset = start
                first_row = 0
                while offset < end:
                    header, header_size = read_page_header(stream, offset, end - offset)
                    (
                        page_type,
                        uncompressed_size,
                        body_size,
                        encoding,
                        num_values,
                        num_rows,
                        repetition_encoding,
                    ) = parse_page_header(header)
                    size = header_size + body_size
                    if size <= 0 or offset + size > end:
                        raise ValueError(
                            "page at {} exceeds chunk [{}, {})".format(offset, start, end)
                        )
                    starts_with_continuation = False
                    if page_type == DATA_PAGE and max_repetition_levels[column] > 0:
                        if repetition_encoding != 3:
                            raise ValueError(
                                "nested V1 page uses unsupported repetition-level encoding "
                                "{}".format(repetition_encoding)
                            )
                        num_rows, starts_with_continuation = v1_page_rows(
                            stream,
                            offset + header_size,
                            body_size,
                            uncompressed_size,
                            chunk.codec,
                            num_values,
                            max_repetition_levels[column],
                        )
                    page_first_row = first_row if page_type in (DATA_PAGE, DATA_PAGE_V2) else None
                    if starts_with_continuation:
                        page_first_row = max(0, page_first_row - 1)
                    page = Page(
                        column, group_ordinal, offset, size, page_type, encoding,
                        None if page_first_row is None else row_base + page_first_row,
                        page_first_row, chunk.codec, num_values or 0,
                        header_size + uncompressed_size
                    )
                    page.starts_with_continuation = starts_with_continuation
                    chunk.pages.append(page)
                    pages.append(page)
                    if page_type in (DATA_PAGE, DATA_PAGE_V2):
                        if num_rows is None:
                            num_rows = num_values
                        first_row += num_rows
                    offset += size
                if offset != end:
                    raise ValueError("column chunk scan did not end at its declared size")
                if first_row != group_rows:
                    raise ValueError(
                        "page rows {} do not match row-group rows {} for chunk ({}, {})".format(
                            first_row, group_rows, column, group_ordinal
                        )
                    )
            row_base += group_rows
    return chunks, pages


def common_prefix(left, right):
    size = min(len(left), len(right))
    index = 0
    while index < size and left[index] == right[index]:
        index += 1
    return left[:index]


def round_up(value):
    output = bytearray(value)
    for index in range(len(output) - 1, -1, -1):
        if output[index] != 0xff:
            output[index] += 1
            return bytes(output[:index + 1])
    return None


def truncate_bounds(minimum, maximum, limit):
    min_exact = True
    max_exact = True
    if limit and len(minimum) > limit:
        minimum = minimum[:limit]
        min_exact = False
    if limit and len(maximum) > limit:
        rounded = round_up(maximum[:limit])
        if rounded is not None:
            maximum = rounded
            max_exact = False
    return minimum, maximum, min_exact, max_exact


def statistic(metadata):
    item = field(metadata, 12)
    if item is None:
        return None
    values = item[2]
    minimum = field(values, 6) or field(values, 2)
    maximum = field(values, 5) or field(values, 1)
    return {
        "null": optional_int(values, 3),
        "nan": optional_int(values, 9),
        "min": None if minimum is None else minimum[2],
        "max": None if maximum is None else maximum[2],
        "min_exact": bool(optional_int(values, 8, True)),
        "max_exact": bool(optional_int(values, 7, True)),
    }


def build_statistics_descriptor(column_chunks, range_start_rows, suffix_limit):
    if len(column_chunks) != len(range_start_rows):
        raise ValueError("statistics ranges do not match column chunks")
    if not range_start_rows or range_start_rows[0] != 0 or any(
        current <= previous
        for previous, current in zip(range_start_rows, range_start_rows[1:])
    ):
        raise ValueError("statistics ranges must start at zero and increase")
    null_positions = []
    null_values = []
    nan_positions = []
    nan_values = []
    minmax_positions = []
    prefixes = []
    min_suffixes = []
    max_suffixes = []
    min_exact = []
    max_exact = []
    for position, chunk in enumerate(column_chunks):
        stats = statistic(chunk.metadata)
        if stats is None:
            return b""
        has_minmax = stats["min"] is not None and stats["max"] is not None
        if stats["null"] is None and stats["nan"] is None and not has_minmax:
            return b""
        if stats["null"] is not None:
            null_positions.append(position)
            null_values.append(stats["null"])
        if stats["nan"] is not None:
            nan_positions.append(position)
            nan_values.append(stats["nan"])
        if not has_minmax:
            continue
        prefix = common_prefix(stats["min"], stats["max"])
        minimum = stats["min"][len(prefix):]
        maximum = stats["max"][len(prefix):]
        minimum, maximum, min_was_exact, max_was_exact = truncate_bounds(
            minimum, maximum, suffix_limit
        )
        minmax_positions.append(position)
        prefixes.append(prefix)
        min_suffixes.append(minimum)
        max_suffixes.append(maximum)
        min_exact.append(int(stats["min_exact"] and min_was_exact))
        max_exact.append(int(stats["max_exact"] and max_was_exact))
    domain = len(range_start_rows)
    fields = [
        [1, compact.BINARY, delta_values(range_start_rows)],
    ]
    if null_positions:
        fields.append(encoded_field(3, sparse_int_array(domain, null_positions, null_values)))
    if minmax_positions:
        fields.extend([
            encoded_field(4, sparse_bytes_array(domain, minmax_positions, prefixes)),
            encoded_field(5, sparse_bytes_array(domain, minmax_positions, min_suffixes)),
            encoded_field(6, sparse_bytes_array(domain, minmax_positions, max_suffixes)),
        ])
        if not all(min_exact) or not all(max_exact):
            fields.extend([
                encoded_field(7, sparse_int_array(domain, minmax_positions, min_exact)),
                encoded_field(8, sparse_int_array(domain, minmax_positions, max_exact)),
            ])
    if nan_positions:
        fields.append(encoded_field(9, sparse_int_array(domain, nan_positions, nan_values)))
    return compact.encode(fields)


def build_legacy_offset_index(chunk):
    locations = []
    for page in chunk.pages:
        if page.page_type not in (DATA_PAGE, DATA_PAGE_V2):
            continue
        if page.starts_with_continuation:
            raise ValueError(
                "legacy OffsetIndex cannot describe a page beginning inside a top-level row"
            )
        locations.append([
            [1, compact.I64, page.offset],
            [2, compact.I32, page.size],
            [3, compact.I64, page.first_row_in_group],
        ])
    return compact.encode([
        [1, compact.LIST, (compact.STRUCT, locations)],
    ])


def add_index_locators(footer, chunks, footer_start):
    converted = copy.deepcopy(footer)
    converted_groups = row_groups(converted)
    offset = footer_start
    index_bytes = 0
    for group_ordinal, group in enumerate(converted_groups):
        for column, thrift_chunk in enumerate(column_chunks(group)):
            blob = build_legacy_offset_index(chunks[group_ordinal][column])
            thrift_chunk[:] = [item for item in thrift_chunk if item[0] not in (4, 5)]
            thrift_chunk.extend([
                [4, compact.I64, offset],
                [5, compact.I32, len(blob)],
            ])
            thrift_chunk.sort(key=lambda item: item[0])
            offset += len(blob)
            index_bytes += len(blob)
    footer_bytes = compact.encode(converted)
    return index_bytes, len(footer_bytes), index_bytes + len(footer_bytes)


class Layout:
    def __init__(self, footer_start, initial_offsets):
        self.footer_start = footer_start
        self.offsets = list(initial_offsets)
        self.output = bytearray()
        self.regions = {}

    @property
    def cursor(self):
        return self.footer_start + len(self.output)

    def add(self, name, blob):
        if not blob:
            raise ValueError("cannot create an empty region")
        if self.offsets[-1] != self.cursor:
            raise ValueError("region layout is not contiguous")
        region = len(self.offsets) - 1
        start = self.cursor
        self.output.extend(blob)
        self.offsets.append(self.cursor)
        self.regions[name] = (region, start, len(blob))
        return region


def initial_region_offsets(pages, footer_start):
    boundaries = {0, footer_start}
    intervals = []
    for page in pages:
        boundaries.add(page.offset)
        boundaries.add(page.offset + page.size)
        intervals.append((page.offset, page.offset + page.size))
    ordered = sorted(boundaries)
    for start, end in intervals:
        index = bisect.bisect_left(ordered, start)
        if index + 1 >= len(ordered) or ordered[index + 1] != end:
            raise ValueError("a page does not occupy exactly one region")
    return ordered


def thrift_schema_module(footer):
    schema = required(footer, 2, compact.LIST, "FileMetaData.schema")
    fields = [[1, compact.LIST, schema]]
    column_orders = field(footer, 7)
    if column_orders is not None:
        fields.append([2, compact.LIST, column_orders[2]])
    return compact.encode(fields)


def thrift_file_metadata_module(footer):
    fields = []
    created_by = field(footer, 6)
    key_values = field(footer, 5)
    if created_by is not None:
        fields.append([1, compact.BINARY, created_by[2]])
    if key_values is not None:
        fields.append([2, compact.LIST, key_values[2]])
    return b"" if not fields else compact.encode(fields)


def build_page_footer(path, footer, footer_start, chunks, pages, suffix_limit):
    groups = row_groups(footer)
    num_columns = len(chunks[0])
    offsets = initial_region_offsets(pages, footer_start)
    region_by_offset = {offset: index for index, offset in enumerate(offsets[:-1])}
    for page in pages:
        page.region = region_by_offset[page.offset]
    layout = Layout(footer_start, offsets)
    modules = []
    page_descriptor_bytes = 0
    stats_descriptor_bytes = 0
    stats_directory_bytes = 0

    column_pages = [[] for unused in range(num_columns)]
    for group_chunks in chunks:
        for chunk in group_chunks:
            column_pages[chunk.column].extend(
                page for page in chunk.pages
                if page.page_type in (DATA_PAGE, DATA_PAGE_V2)
            )

    page_regions = []
    for column in range(num_columns):
        for group_chunks in chunks:
            chunk = group_chunks[column]
            dictionary = next(
                (page for page in chunk.pages if page.page_type == DICTIONARY_PAGE), None
            )
            dictionary_region = None if dictionary is None else dictionary.region
            for page in chunk.pages:
                if page.page_type not in (DATA_PAGE, DATA_PAGE_V2):
                    continue
                uses_dictionary = page.encoding in (PLAIN_DICTIONARY, RLE_DICTIONARY)
                if uses_dictionary and dictionary_region is None:
                    raise ValueError("dictionary-encoded page has no dictionary page")
                page.dictionary_region = dictionary_region if uses_dictionary else None

        column_page_values = column_pages[column]
        page_blob = compact.encode([
            [1, compact.I32, len(column_page_values)],
            [
                2, compact.BINARY,
                dense_values([page.region for page in column_page_values]),
            ],
            [
                3, compact.BINARY,
                dense_values([page.first_row for page in column_page_values]),
            ],
            [4, compact.BOOL_TRUE, bool(column_page_values) and all(
                page.dictionary_region is not None for page in column_page_values
            )],
            [
                5, compact.BINARY,
                delta_values([page.codec for page in column_page_values]),
            ],
            [6, compact.BINARY, dense_values([
                0 if page.dictionary_region is None else page.dictionary_region + 1
                for page in column_page_values
            ])],
            [
                7, compact.BINARY,
                dense_values([page.num_values for page in column_page_values]),
            ],
            [8, compact.BINARY, dense_values([
                page.total_uncompressed_size for page in column_page_values
            ])],
        ])
        page_regions.append(layout.add("pages_{}".format(column), page_blob))
        page_descriptor_bytes += len(page_blob)

    page_directory = compact.encode([
        encoded_field(1, dense_array(page_regions)),
    ])
    page_directory_region = layout.add("page_directory", page_directory)
    modules.append((K_PAGE_DIRECTORY, page_directory_region))

    stats_regions = []
    stats_positions = []
    stats_values = []
    row_start = 0
    range_starts = []
    for group in groups:
        num_rows = required(group, 3, compact.I64, "RowGroup.num_rows")
        range_starts.append(row_start)
        row_start += num_rows
    for column in range(num_columns):
        descriptor = build_statistics_descriptor(
            [group_chunks[column] for group_chunks in chunks],
            range_starts,
            suffix_limit,
        )
        if descriptor:
            region = layout.add("statistics_{}".format(column), descriptor)
            stats_positions.append(column)
            stats_values.append(region)
            stats_descriptor_bytes += len(descriptor)
    if stats_positions:
        stats_directory = compact.encode([
            encoded_field(
                1,
                sparse_int_array(num_columns, stats_positions, stats_values),
            ),
        ])
        stats_directory_bytes = len(stats_directory)
        stats_directory_region = layout.add("column_range_statistics", stats_directory)
        modules.append((K_COLUMN_RANGE_STATISTICS, stats_directory_region))

    file_metadata_blob = thrift_file_metadata_module(footer)
    if file_metadata_blob:
        file_metadata_region = layout.add("file_metadata", file_metadata_blob)
        modules.append((K_FILE_METADATA, file_metadata_region))

    schema_blob = thrift_schema_module(footer)
    schema_region = layout.add("schema", schema_blob)
    modules.append((K_SCHEMA, schema_region))

    region_map_offset = layout.cursor
    region_map_blob = compact.encode([
        [1, compact.STRUCT, delta_array(layout.offsets)],
    ])
    root_offset = region_map_offset + len(region_map_blob)
    root_blob = compact.encode([
        [1, compact.I32, required(footer, 1, compact.I32, "FileMetaData.version")],
        [2, compact.I32, num_columns],
        [3, compact.I64, required(footer, 3, compact.I64, "FileMetaData.num_rows")],
        [4, compact.LIST, (
            compact.STRUCT,
            [
                [
                    [1, compact.I32, kind],
                    [2, compact.I64, region],
                ]
                for kind, region in modules
            ],
        )],
    ])
    decoded_region_map = compact.decode(region_map_blob)
    decoded_offsets = decode_delta_array(
        required(decoded_region_map, 1, compact.STRUCT, "RegionMap.region_offsets")
    )
    if decoded_offsets != layout.offsets:
        raise ValueError("RegionMap round-trip changed an offset")
    decoded_root = compact.decode(root_blob)
    if compact.encode(decoded_root) != root_blob:
        raise ValueError("page-first root failed compact-Thrift round-trip")
    metadata = bytes(layout.output) + region_map_blob + root_blob
    addressing_bytes = page_descriptor_bytes + len(page_directory) + len(region_map_blob)
    placement_bytes = addressing_bytes
    details = {
        "metadata": metadata,
        "region_map_offset": region_map_offset,
        "region_map_bytes": len(region_map_blob),
        "root_offset": root_offset,
        "root_bytes": len(root_blob),
        "placement_bytes": placement_bytes,
        "addressing_bytes": addressing_bytes,
        "page_descriptor_bytes": page_descriptor_bytes,
        "stats_descriptor_bytes": stats_descriptor_bytes,
        "statistics_bytes": stats_descriptor_bytes + stats_directory_bytes,
        "module_regions": layout.regions,
        "num_regions": len(layout.offsets) - 1,
    }
    return details


def write_output(path, output, footer_start, details, full_file):
    trailer = struct.pack(
        "<qqqq4s",
        details["region_map_offset"],
        details["region_map_bytes"],
        details["root_offset"],
        details["root_bytes"],
        b"PMF1",
    )
    with output.open("wb") as destination:
        if full_file:
            with path.open("rb") as source:
                remaining = footer_start
                while remaining:
                    block = source.read(min(1024 * 1024, remaining))
                    if not block:
                        raise ValueError("unexpected end while copying data prefix")
                    destination.write(block)
                    remaining -= len(block)
        destination.write(details["metadata"])
        destination.write(trailer)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path, nargs="?")
    parser.add_argument("--full-file", action="store_true")
    parser.add_argument("--truncate-minmax", type=int, default=16)
    args = parser.parse_args()
    output = args.output or args.input.with_suffix(args.input.suffix + ".page-modular")

    unused_file_size, footer_start, footer_bytes, footer = read_footer(args.input)
    chunks, pages = scan_pages(args.input, footer)
    details = build_page_footer(
        args.input,
        footer,
        footer_start,
        chunks,
        pages,
        args.truncate_minmax,
    )
    legacy_index_bytes, legacy_footer_bytes, legacy_total = add_index_locators(
        footer, chunks, footer_start
    )
    write_output(args.input, output, footer_start, details, args.full_file)

    num_chunks = sum(len(group_chunks) for group_chunks in chunks)
    data_pages = sum(
        page.page_type in (DATA_PAGE, DATA_PAGE_V2) for page in pages
    )
    dictionary_pages = sum(page.page_type == DICTIONARY_PAGE for page in pages)
    print("column_chunks {}".format(num_chunks))
    print("data_pages {}".format(data_pages))
    print("dictionary_pages {}".format(dictionary_pages))
    print("page_regions {}".format(data_pages + dictionary_pages))
    print("regions {}".format(details["num_regions"]))
    print("standard_footer_bytes {}".format(len(footer_bytes)))
    print("legacy_offset_index_bytes {}".format(legacy_index_bytes))
    print("legacy_footer_with_index_locators_bytes {}".format(legacy_footer_bytes))
    print("legacy_footer_and_offset_index_bytes {}".format(legacy_total))
    print("page_addressing_bytes {}".format(details["addressing_bytes"]))
    print("page_placement_bytes {}".format(details["placement_bytes"]))
    print("page_statistics_bytes {}".format(details["statistics_bytes"]))
    print("page_modular_total_bytes {}".format(len(details["metadata"])))
    for name, (region, offset, length) in details["module_regions"].items():
        print("{} region={} off={} len={}".format(name, region, offset, length))
    print("region_map off={} len={}".format(
        details["region_map_offset"], details["region_map_bytes"]
    ))
    print("root off={} len={}".format(details["root_offset"], details["root_bytes"]))
    print("output {}".format(output))
    return 0


if __name__ == "__main__":
    sys.exit(main())

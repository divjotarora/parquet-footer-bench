#!/usr/bin/env python3
"""Read exact standard-footer sizes from downloaded Parquet files."""

import argparse
import json
import os
import struct
import subprocess
import sys
import tempfile
from pathlib import Path

import compact


ROOT = Path(__file__).resolve().parent


def footer_size(path):
    file_bytes = path.stat().st_size
    if file_bytes < 12:
        raise ValueError("file is too short")
    with path.open("rb") as stream:
        if stream.read(4) != b"PAR1":
            raise ValueError("missing leading PAR1 magic")
        stream.seek(-8, os.SEEK_END)
        trailer = stream.read(8)
    if trailer[4:] != b"PAR1":
        raise ValueError("missing trailing PAR1 magic")
    footer_bytes = struct.unpack("<I", trailer[:4])[0]
    if footer_bytes + 12 > file_bytes:
        raise ValueError("footer length exceeds file size")
    return file_bytes, footer_bytes


def read_footer(path):
    file_bytes, footer_bytes = footer_size(path)
    with path.open("rb") as stream:
        stream.seek(-8 - footer_bytes, os.SEEK_END)
        footer = stream.read(footer_bytes)
    return file_bytes, footer


def field(fields, field_id):
    for item in fields:
        if item[0] == field_id:
            return item
    return None


def column_metadata(footer):
    row_groups = field(footer, 4)
    if row_groups is None or row_groups[1] != compact.LIST:
        raise ValueError("FileMetaData.row_groups is missing")
    for row_group in row_groups[2][1]:
        columns = field(row_group, 1)
        if columns is None or columns[1] != compact.LIST:
            raise ValueError("RowGroup.columns is missing")
        for column_chunk in columns[2][1]:
            metadata = field(column_chunk, 3)
            if metadata is None or metadata[1] != compact.STRUCT:
                raise ValueError("external ColumnChunk.file_path is not supported")
            yield metadata[2]


def without_paths(footer):
    removed = 0
    for metadata in column_metadata(footer):
        before = len(metadata)
        metadata[:] = [item for item in metadata if item[0] != 3]
        removed += before - len(metadata)
    return removed


def common_prefix(left, right):
    size = min(len(left), len(right))
    offset = 0
    while offset < size and left[offset] == right[offset]:
        offset += 1
    return left[:offset]


def prefix_statistics(footer, suffix_limit):
    converted = 0
    prefix_bytes = 0
    suffix_bytes = 0
    truncated = 0
    for metadata in column_metadata(footer):
        statistics_field = field(metadata, 12)
        if statistics_field is None:
            continue
        statistics = statistics_field[2]
        by_id = {item[0]: item for item in statistics}
        min_field = by_id.get(6) or by_id.get(2)
        max_field = by_id.get(5) or by_id.get(1)
        if min_field is None or max_field is None:
            continue
        if min_field[1] != compact.BINARY or max_field[1] != compact.BINARY:
            raise ValueError("statistics bounds are not binary")
        minimum = min_field[2]
        maximum = max_field[2]
        prefix = common_prefix(minimum, maximum)
        min_remainder = minimum[len(prefix):]
        max_remainder = maximum[len(prefix):]
        min_suffix = min_remainder[:suffix_limit]
        max_suffix = max_remainder[:suffix_limit]
        was_truncated = len(min_remainder) > suffix_limit or len(max_remainder) > suffix_limit

        # Experimental Statistics representation:
        #   1: common prefix, 2: min suffix, 5: max suffix.
        # Standard min/max fields 1, 2, 5, and 6 are removed first. Other
        # statistics fields (null count, distinct count, exactness) remain.
        statistics[:] = [item for item in statistics if item[0] not in (1, 2, 5, 6)]
        statistics.extend([
            [1, compact.BINARY, prefix],
            [2, compact.BINARY, min_suffix],
            [5, compact.BINARY, max_suffix],
        ])
        statistics.sort(key=lambda item: item[0])
        if was_truncated:
            for exactness_id in (7, 8):
                exactness = field(statistics, exactness_id)
                if exactness is not None:
                    exactness[2] = False
        converted += 1
        prefix_bytes += len(prefix)
        suffix_bytes += len(min_suffix) + len(max_suffix)
        truncated += int(was_truncated)
    return converted, prefix_bytes, suffix_bytes, truncated


def measure(path, suffix_limit):
    unused_file_bytes, standard = read_footer(path)
    decoded = compact.decode(standard)
    if compact.encode(decoded) != standard:
        raise ValueError("compact-Thrift fidelity check failed for {}".format(path))
    path_fields = without_paths(decoded)
    no_path = compact.encode(decoded)
    converted, prefix_bytes, suffix_bytes, truncated = prefix_statistics(decoded, suffix_limit)
    prefix_stats = compact.encode(decoded)
    return {
        "standard": len(standard),
        "no_path": len(no_path),
        "prefix_suffix": len(prefix_stats),
        "path_fields": path_fields,
        "stats_converted": converted,
        "stats_truncated": truncated,
        "prefix_bytes": prefix_bytes,
        "suffix_bytes": suffix_bytes,
    }


def oss_breakdown(path):
    """Split the OSS footer by progressively clearing fields; parts sum exactly."""
    unused_file_bytes, standard = read_footer(path)
    decoded = compact.decode(standard)
    if compact.encode(decoded) != standard:
        raise ValueError("compact-Thrift fidelity check failed for {}".format(path))

    without_paths(decoded)
    without_path = len(compact.encode(decoded))

    for metadata in column_metadata(decoded):
        metadata[:] = [item for item in metadata if item[0] != 12]
    without_stats = len(compact.encode(decoded))

    decoded[:] = [item for item in decoded if item[0] != 4]
    without_placement = len(compact.encode(decoded))

    decoded[:] = [item for item in decoded if item[0] != 2]
    without_schema = len(compact.encode(decoded))

    decoded[:] = [item for item in decoded if item[0] != 5]
    other = len(compact.encode(decoded))
    return {
        "path_in_schema": len(standard) - without_path,
        "statistics": without_path - without_stats,
        "placement": without_stats - without_placement,
        "schema": without_placement - without_schema,
        "key_value_metadata": without_schema - other,
        "other": other,
        "total": len(standard),
    }


def modular_measure(path, converter, suffix_limit):
    handle, output_name = tempfile.mkstemp(prefix="modular-footer-", suffix=".bin")
    os.close(handle)
    output = Path(output_name)
    command = [
        str(converter),
        "--truncate-minmax={}".format(suffix_limit),
        str(path),
        str(output),
    ]
    try:
        completed = subprocess.run(
            command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True
        )
        values = {}
        modules = {}
        for line in completed.stdout.splitlines():
            pieces = line.split()
            if len(pieces) == 2 and pieces[0] in ("modular_total_bytes", "column_chunks"):
                values[pieces[0]] = int(pieces[1])
            if len(pieces) == 3 and pieces[1].startswith("off=") and pieces[2].startswith("len="):
                modules[pieces[0]] = {
                    "offset": int(pieces[1].split("=", 1)[1]),
                    "length": int(pieces[2].split("=", 1)[1]),
                }
        measured_size = output.stat().st_size
        # The converter writes the modular blob (modular_total_bytes) followed by a small
        # locator trailer (12 bytes metadata-only, 20 full-file). The footer-size metric is
        # the blob; verify the file is the blob plus that trailer, then report the blob size.
        total = values.get("modular_total_bytes")
        if total is None or not 0 <= measured_size - total <= 20:
            raise ValueError("converter output size does not match its report")
        serialized = output.read_bytes()
        measured_size = total
        module_bytes = {
            name: serialized[module["offset"]:module["offset"] + module["length"]]
            for name, module in modules.items()
        }
        return {
            "bytes": measured_size,
            "column_chunks": values.get("column_chunks"),
            "modules": modules,
            "module_bytes": module_bytes,
        }
    finally:
        if output.exists():
            output.unlink()


def modular_breakdown(path, converter, suffix_limit):
    measured = modular_measure(path, converter, suffix_limit)
    modules = measured["modules"]
    schema = modules.get("schema", {}).get("length", 0)
    placement = modules.get("placement", {}).get("length", 0)
    key_value_metadata = 0
    file_metadata = measured["module_bytes"].get("file_metadata")
    if file_metadata is not None:
        decoded = compact.decode(file_metadata)
        without_key_values = [item for item in decoded if item[0] != 2]
        key_value_metadata = len(file_metadata) - len(compact.encode(without_key_values))
    statistics = 0
    stats = modules.get("row_group_stats")
    if stats is not None:
        preceding_end = max(
            (
                module["offset"] + module["length"]
                for name, module in modules.items()
                if module["offset"] < stats["offset"] and name != "row_group_stats"
            ),
            default=0,
        )
        statistics = stats["offset"] + stats["length"] - preceding_end
    other = measured["bytes"] - schema - placement - statistics - key_value_metadata
    return {
        "path_in_schema": 0,
        "schema": schema,
        "placement": placement,
        "statistics": statistics,
        "key_value_metadata": key_value_metadata,
        "other": other,
        "total": measured["bytes"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("names", nargs="*", help="dataset names; default: all")
    parser.add_argument("--manifest", type=Path, default=ROOT / "corpus.json")
    parser.add_argument("--data", type=Path, default=ROOT / "data")
    parser.add_argument("--suffix-limit", type=int, default=16)
    parser.add_argument("--csv", action="store_true")
    parser.add_argument(
        "--modular-converter",
        type=Path,
        default=ROOT.parent / "build" / "modular_footer_convert",
    )
    args = parser.parse_args()
    entries = json.loads(args.manifest.read_text())["datasets"]
    by_name = {entry["name"]: entry for entry in entries}
    selected = [by_name[name] for name in args.names] if args.names else entries

    if args.csv:
        print("name,standard,no_path,prefix_suffix,modular,path_saving,stats_saving")
    else:
        print("{:<32} {:>12} {:>12} {:>12} {:>12} {:>10} {:>10}".format(
            "name", "standard", "no_path", "prefix_16", "modular", "path save", "stat save"
        ))
    for entry in selected:
        path = args.data / (entry["name"] + ".parquet")
        result = measure(path, args.suffix_limit)
        modular = modular_measure(path, args.modular_converter, args.suffix_limit)
        path_saving = 1 - result["no_path"] / result["standard"]
        stats_saving = 1 - result["prefix_suffix"] / result["no_path"]
        if args.csv:
            print("{},{},{},{},{},{:.8f},{:.8f}".format(
                entry["name"], result["standard"], result["no_path"],
                result["prefix_suffix"], modular["bytes"], path_saving, stats_saving
            ))
        else:
            print("{:<32} {:>12,d} {:>12,d} {:>12,d} {:>12,d} {:>9.2%} {:>9.2%}".format(
                entry["name"], result["standard"], result["no_path"],
                result["prefix_suffix"], modular["bytes"], path_saving, stats_saving
            ))
        if result["standard"] != entry["footer_bytes"]:
            raise ValueError("{} footer differs from manifest".format(entry["name"]))
        if not args.csv:
            print("  stats: {} converted, {} truncated, {:,} prefix bytes, "
                  "{:,} suffix bytes".format(
                      result["stats_converted"], result["stats_truncated"],
                      result["prefix_bytes"], result["suffix_bytes"]
                  ))
    return 0


if __name__ == "__main__":
    sys.exit(main())

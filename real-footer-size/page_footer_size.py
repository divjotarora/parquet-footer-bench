#!/usr/bin/env python3
"""Compare legacy and page-first footer metadata on the pinned real corpus."""

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import footer_size


ROOT = Path(__file__).resolve().parent


def modular_statistics_bytes(measured):
    modules = measured["modules"]
    stats = modules.get("row_group_stats")
    if stats is None:
        return 0
    preceding_end = max(
        (
            module["offset"] + module["length"]
            for name, module in modules.items()
            if module["offset"] < stats["offset"] and name != "row_group_stats"
        ),
        default=0,
    )
    return stats["offset"] + stats["length"] - preceding_end


def measure(path, converter, modular_converter, suffix_limit):
    with tempfile.NamedTemporaryFile(suffix=".page-modular", delete=False) as output:
        output_path = Path(output.name)
    command = [
        sys.executable,
        str(converter),
        "--truncate-minmax",
        str(suffix_limit),
        str(path),
        str(output_path),
    ]
    try:
        completed = subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        values = {}
        for line in completed.stdout.splitlines():
            pieces = line.split()
            if len(pieces) == 2 and pieces[1].isdigit():
                values[pieces[0]] = int(pieces[1])
        required = (
            "column_chunks",
            "data_pages",
            "dictionary_pages",
            "standard_footer_bytes",
            "legacy_offset_index_bytes",
            "legacy_footer_with_index_locators_bytes",
            "legacy_footer_and_offset_index_bytes",
            "page_placement_bytes",
            "page_statistics_bytes",
            "page_modular_total_bytes",
        )
        missing = [name for name in required if name not in values]
        if missing:
            raise ValueError("converter omitted {}".format(", ".join(missing)))
        values["total_pages"] = values["data_pages"] + values["dictionary_pages"]
        modular = footer_size.modular_measure(path, modular_converter, suffix_limit)
        values["modular_placement_bytes"] = modular["modules"]["placement"]["length"]
        values["modular_statistics_bytes"] = modular_statistics_bytes(modular)
        values["modular_total_bytes"] = modular["bytes"]
        return values
    finally:
        if output_path.exists():
            output_path.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("names", nargs="*", help="dataset names; default: all")
    parser.add_argument("--manifest", type=Path, default=ROOT / "corpus.json")
    parser.add_argument("--data", type=Path, default=ROOT / "data")
    parser.add_argument("--suffix-limit", type=int, default=16)
    parser.add_argument("--csv", action="store_true")
    parser.add_argument(
        "--converter",
        type=Path,
        default=ROOT.parent / "modular-footer" / "parquet_to_page_modular.py",
    )
    parser.add_argument(
        "--modular-converter",
        type=Path,
        default=ROOT.parent / "build" / "modular_footer_convert",
    )
    args = parser.parse_args()

    entries = json.loads(args.manifest.read_text())["datasets"]
    by_name = {entry["name"]: entry for entry in entries}
    unknown = sorted(set(args.names) - set(by_name))
    if unknown:
        parser.error("unknown dataset(s): {}".format(", ".join(unknown)))
    selected = [by_name[name] for name in args.names] if args.names else entries

    if args.csv:
        print(
            "name,column_chunks_count,data_pages_count,dictionary_pages_count,total_pages_count,"
            "legacy_footer_bytes,legacy_offset_index_bytes,legacy_footer_with_locators_bytes,"
            "legacy_footer_and_index_bytes,modular_placement_bytes,modular_total_bytes,"
            "modular_statistics_bytes,page_placement_bytes,page_statistics_bytes,"
            "page_modular_total_bytes"
        )
    else:
        print(
            "{:<32} {:>10} {:>10} {:>12} {:>14} {:>14} {:>14} {:>14} {:>14}".format(
                "name", "chunks #", "pages #", "legacy B", "legacy + OI B", "mod place B",
                "mod total B", "page place B", "page total B"
            )
        )

    for entry in selected:
        path = args.data / (entry["name"] + ".parquet")
        values = measure(
            path, args.converter, args.modular_converter, args.suffix_limit
        )
        if values["standard_footer_bytes"] != entry["footer_bytes"]:
            raise ValueError("{} footer differs from manifest".format(entry["name"]))
        pages = values["total_pages"]
        if args.csv:
            print(
                "{name},{column_chunks},{data_pages},{dictionary_pages},{total_pages},"
                "{standard_footer_bytes},{legacy_offset_index_bytes},"
                "{legacy_footer_with_index_locators_bytes},"
                "{legacy_footer_and_offset_index_bytes},{modular_placement_bytes},"
                "{modular_total_bytes},{modular_statistics_bytes},{page_placement_bytes},"
                "{page_statistics_bytes},"
                "{page_modular_total_bytes}".format(name=entry["name"], **values)
            )
        else:
            print(
                "{:<32} {:>10,d} {:>10,d} {:>12,d} {:>14,d} {:>14,d} {:>14,d} "
                "{:>14,d} {:>14,d}".format(
                    entry["name"],
                    values["column_chunks"],
                    pages,
                    values["standard_footer_bytes"],
                    values["legacy_footer_and_offset_index_bytes"],
                    values["modular_placement_bytes"],
                    values["modular_total_bytes"],
                    values["page_placement_bytes"],
                    values["page_modular_total_bytes"],
                )
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())

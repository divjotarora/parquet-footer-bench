#!/usr/bin/env python3
"""Create a wide Parquet file with real pages for footer decode benchmarks."""

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--columns", type=int, default=2000)
    parser.add_argument("--row-groups", type=int, default=10)
    parser.add_argument("--rows-per-group", type=int, default=1000)
    args = parser.parse_args()

    rows = args.row_groups * args.rows_per_group
    values = pa.array(range(rows), type=pa.int64())
    names = ["c{:04d}".format(column) for column in range(args.columns)]
    table = pa.Table.from_arrays([values] * args.columns, names=names)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table,
        args.output,
        compression="snappy",
        data_page_size=1024 * 1024,
        row_group_size=args.rows_per_group,
        use_dictionary=True,
        write_statistics=True,
    )
    print("wrote {}: {} columns x {} row groups x {} rows/group".format(
        args.output, args.columns, args.row_groups, args.rows_per_group
    ))


if __name__ == "__main__":
    main()

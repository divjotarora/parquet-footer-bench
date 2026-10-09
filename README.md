# parquet-footer-bench

A small, dependency-free playground for **experimenting with Parquet footer layouts** and
measuring them — not estimating them. A Parquet reader, before it can fetch column data,
needs the *placement* of every column chunk (where it starts, how big it is) and often has
to resolve query column *names* to ordinals first. Today all of that lives in one nested
Thrift `FileMetaData` footer. This repo makes the trade-offs of encoding it differently
concrete: every number comes from a real serialize → parse → resolve round-trip, and a
fidelity cross-check proves each layout resolves to the identical answer before any timing.

No third-party dependencies (the Thrift Compact codec is vendored). Build everything from
the repo root with any C++17 compiler:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build
```

## What's here

| Directory | What it is |
|---|---|
| [`pfb-harness/`](pfb-harness/) | The footer-layout harness: **synthesizes** footer shapes in memory and measures placement `build`/`resolve`/size across candidate layouts, with a fidelity cross-check. `pfb_bench`, `pfb_fidelity_test`. |
| [`footer-decode-bench/`](footer-decode-bench/) | Two plan-only micro-benchmarks on **real** converted footers: decode speed across four layouts, and column-name resolution (walk vs. a persisted name hash). |
| [`real-footer-size/`](real-footer-size/) | Footer **size** on real Parquet files. |
| `jumptable-footer/`, `modular-footer/` | The footer-variation **definitions** (Thrift) and the converters that turn a real Parquet file into each variation. This includes the page-first [design](modular-footer/PageFirstModularFooter.md) and [Thrift schema](modular-footer/PageFirstModularFooter.thrift), plus the current modular footer's optional `SCHEMA_INDEX` module. |
| [`include/pfb/`](include/pfb/) | `bitpack.h` — shared LSB bit-packing used by the harness and the converters. |

## License

Apache License 2.0. See [LICENSE](LICENSE).

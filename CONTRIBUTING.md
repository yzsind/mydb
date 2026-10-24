# Contributing to MyDB

Thanks for your interest in improving MyDB. This page covers how to build, test, and submit changes.

## Prerequisites

- Rust 1.95 or newer (`rust-toolchain.toml` pins the exact toolchain used by CI).
- A native C toolchain and Perl for the vendored OpenSSL authentication backend.
- A MySQL client (`mysql`) for smoke-testing the server.
- Python 3 for build, release, and correctness verification scripts under `scripts/`.

## Build and test

```bash
cargo build --release --locked --bin mydb     # server binary
cargo nextest run                             # local regression tests (default)
cargo nextest run --no-default-features --features tls,fast-alloc # no MySQL receiver
cargo build --features tools                  # benchmark and diagnostic tools
```

A quick smoke test:

```bash
MYDB_DATA_DIR=/tmp/mydb-data cargo run -- --host 127.0.0.1 --port 3307
mysql -h127.0.0.1 -P3307 -uroot -proot test
```

Use a temporary `MYDB_DATA_DIR` for experiments; do not commit generated data, logs, or benchmark outputs.

## What CI enforces

Pull requests run the same gates as the main branch:

- `cargo fmt --all -- --check`
- `cargo check --all-targets --all-features`, plus the feature matrix (`--no-default-features`, `tools`, `--all-features`)
- `cargo clippy` with `correctness` and `suspicious` lints denied
- `cargo nextest run` on default features and without the MySQL replication receiver
- Local-process native replica recovery, text/binary index aggregate metadata, and physical backup gates
- MSRV check on Rust 1.95 and a macOS compile + release smoke test
- A MySQL MTR-derived semantic non-regression budget, which may only move downward
- Dependency advisories, licenses, and registry sources (`cargo deny --locked --all-features check advisories licenses sources`)

The main CI runs on pull requests and pushes to `main`. Linux glibc 2.17 and
deep safety workflows run on `v*` tags or manual dispatch. Repository rules must
separately require successful checks before merging; workflow files do not set
branch protection. Local development and release acceptance records are kept
in `docs-internal/`, which is excluded from Git.

Run at least `cargo fmt --all` and `cargo nextest run` before submitting.

## Documentation

Keep product usage, SQL references, deployment guides, and stable architecture
specifications in `docs/`. Keep development plans, audits, fix logs, test
procedures, acceptance evidence, and historical benchmark results in
`docs-internal/`. This local directory is ignored by Git and must not be force-added.
Public documentation must not link to files that exist only in that directory.

## Project principles

- **General purpose, not benchmark-shaped.** TPC-C, sysbench, TPC-H, TPC-DS, ClickBench and similar suites are measurement tools only; the code must not special-case them.
- **Fail loudly.** Unsupported SQL, protocol features, and syntax must return clear errors — never a silent no-op.
- **Correctness first, then performance.** Optimizations must be general across data types, functions, and syntax, and must not regress existing behavior.
- **Reproduce before fixing.** Bug fixes should demonstrate the failure first (a test or a reproducible SQL sequence), then fix it, then verify the fix.

## Pull requests

- Keep changes focused; describe the behavior change and how it was verified.
- Add or extend regression tests for behavior changes.
- Follow the formatting and idiom of the surrounding code.
- Both English and Chinese are fine for issues and pull requests. Product documentation under `docs/`, code comments, and commit messages use English.

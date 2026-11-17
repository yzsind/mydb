# Build, Release, and Correctness Scripts

This directory contains versioned build and release tools, code generators,
compatibility oracles, benchmark inputs, and the regressions wired into CI.
CI and Docker builds require only public files. Machine-local benchmarks,
profiling tools, comparison-server helpers, historical audit regressions, and
their harness tests live in the ignored `scripts-internal/` directory. They
are optional and are not included in a public checkout.

Scripts run locally by default. Temporary databases, caches, logs, and samples
belong under `/tmp/mydb`. Protocol tests may create or rebuild dedicated databases;
read each entry point's docstring and `--help`, and use disposable instances.
Root tool binaries require `--features tools`; development binaries use
`cargo run -p mydb-dev-tools --release --bin NAME -- ARGS`. Shared Python
runtime and offline test dependencies are pinned in `scripts/requirements.txt`.
Use Python 3.12 or later.

Release archives use `mydb-VERSION-macos-arm64.tar.gz`,
`mydb-VERSION-linux-x86_64.tar.gz`, and `mydb-VERSION-windows-x86_64.zip`.
Each archive contains a single directory matching its filename without the archive
extension. `release_archive.py` shares naming and layout between native and portable
Linux builds; `verify_release_package.py` checks this layout before extraction.
Runtime archives contain only the three executables, a short `README.md`, `LICENSE`,
and `third-party/`. Platform `*.build-manifest.json`, Linux `*.abi.json`, and
`mydb-VERSION.Cargo.lock` and `mydb-VERSION-cli.Cargo.lock` are separate attachments;
`SHA256SUMS` covers the archives and build evidence. The verifier reads the matching
manifest and lockfiles beside the archive. Use `--files-only` for another platform's
archive; native execution checks remain enabled by default.

## Common entry points

| Purpose | Entry point |
|---|---|
| Portable Linux build and ABI contract | `build_linux_glibc217.py`, `check_elf_abi.py`, `verify_linux_compat.py` |
| Host-native release packaging and licenses | `package_release.py`, `release_licenses.py` |
| Extracted release package, version, TLS and crash recovery checks | `verify_release_package.py --package ARCHIVE --output /tmp/mydb/package-check` |
| Windows checks and native isolated tests | `check_windows.py`, `test_windows_native.ps1` |
| Ubuntu development environment | `setup_ubuntu.sh` |
| Build and runtime-matrix command regressions | `test_release_build.py`, `test_linux_compat.py`, `test_elf_abi.py` |
| Native replication recovery and binary protocol | `test_native_readiness.py --binary target/debug/mydb` |
| Acknowledged-state and interrupted-DDL crash recovery | `test_crash_recovery.py --binary target/release/mydb --output /tmp/mydb/crash-recovery --rounds 10`; requires PyMySQL and retains an independent typed-row oracle |
| Physical backup and WAL recovery | `test_backup_recovery.py --profile debug --rows 1000 --pairs 1`; add `--encrypted` for encrypted backups |
| Security, storage failure, atomic publication, and promotion | `test_release_security.py`, `test_release_storage.py`, `test_release_publication.py`, `test_release_promotion.py`; instrumentation sources remain in `fixtures/` |
| Replication consistency under load | `run_repl_tpcc_consistency.sh`, `run_mysql_repl_tpcc_consistency.sh`, `run_mydb_master_mysql_replica.sh`, `check_benchmarksql_consistency.sql` |
| Client tooling compatibility | `run_client_compat_matrix.sh`; see [client examples](../examples/README.md) |
| MySQL 8.0 capture, diff, and generated catalogs | `capture_mysql80_*.py`, `mysql80_*_diff.*`, `generate_mysql80_*.py` |
| Public documentation links and heading anchors | `check_docs.py`; rejects links requiring private files |
| Function and collation coverage | `check_function_test_coverage.py`, `generate_legacy_general_weights.py`, `generate_uca9_weights.py` |
| Function/expression performance comparison | `function_expression_bench.py`, `function_expression_report.py` |
| Local pagination performance comparison | `pagination_query_bench.py --directory /tmp/mydb/pagination-run`; isolated mydb and MySQL 8.0, result checks, plans, and latency samples |
| Storage format documentation, MTR import, and TSAN | `render_storage_format_versions.sh`, `mtr_import.sh`, `run_tsan_btree.sh` |
| SQL oracle inputs and result checks | `tpch/`, `tpcds/`, `clickbench/`, `*_check_results.py`, `clickbench_results.py` |

## Retained regressions and fixtures

Regressions kept here are the ones CI and release tooling run: build and ABI
contracts, backup and recovery, release security/storage/publication/promotion,
native readiness, and the crash-consistency gate that `test_script_maintenance.py`
exercises over `crash_test_index_defer.sh`. Shared protocol and process fixtures
(`protocol_helpers.py`, `fixtures/`) also remain here so public regressions do
not depend on private tools. Historical audit drivers and one-off verification
regressions (`verify_*_audit.py`, `test_*_audit*.py`, `*_audit_verify.py`, and
similar) live in `scripts-internal/` and are not needed by public builds or
default correctness runs.

Pinned MySQL 8.0.46 catalog and status baselines live in
`tests/fixtures/mysql80/`. They are tracked regression inputs used by Rust tests
and catalog generators; they must remain available in a clean checkout.
The information-schema and status capture scripts default to that directory,
independently of the current working directory. Performance-schema capture
requires an explicit `--output` path. Capturing new baselines requires the
pinned reference server; generator checks use the committed JSON files offline.

`storage_oracle.py` provides the physical heap-version and WAL-frame checks used
by publication and promotion gates. It rejects unsupported formats, bad bounds,
and checksums explicitly; `test_storage_oracle.py` runs dependency-free negative
controls. The crash-recovery gate covers both buffered and eager WAL paths
(`--buffering 1|0`), including fresh writes after recovery and a second SIGKILL.
Timed DDL interruption supplements deterministic syscall fault gates; it does
not prove that every internal publication boundary was reached.

Query schemas, SQL files, and externally generated expected TSV files are
regression inputs. Rust parser and executor tests use some TPC-DS files through
`include_str!`; keep their public paths intact. Oracle generators and result
checkers also remain public. These inputs are community adaptations, not
TPC-audited performance results.

## Local checks

```bash
mkdir -p /tmp/mydb
python3 -m venv /tmp/mydb/scripts-venv
TMPDIR=/tmp/mydb PIP_CACHE_DIR=/tmp/mydb/pip-cache \
  /tmp/mydb/scripts-venv/bin/python -m pip install -r scripts/requirements.txt
PYTHONDONTWRITEBYTECODE=1 TMPDIR=/tmp/mydb \
  /tmp/mydb/scripts-venv/bin/python scripts/run_script_tests.py
/tmp/mydb/scripts-venv/bin/python scripts/check_documentation_contracts.py
python3 scripts/check_function_test_coverage.py --check
python3 scripts/check_docs.py
python3 scripts/generate_mysql80_information_schema_rust.py --check
python3 scripts/generate_mysql80_performance_schema_rust.py --check
scripts/render_storage_format_versions.sh --check
```

`run_script_tests.py` automatically loads `test_*.py` modules declaring unittest
cases, while keeping standalone integration drivers out of import discovery.
Import errors and failing cases fail the command. Its JSON evidence records
Python/platform, dependency versions, source hashes and per-run test counts.
CI uses the same entry point and dependency pins.

### Result comparison contracts

TPC-H and TPC-DS checkers compare exact UTF-8 TSV cells and row order by default.
They preserve empty cells/rows, whitespace, duplicates, numeric-looking strings,
and case-sensitive text. Invalid UTF-8, inconsistent widths, and empty,
duplicated, reversed or out-of-range query selections fail explicitly.

Use `--comparison-manifest FILE` for explicit per-query JSON contracts, for example:

```json
{"1": {"columns": ["text", "decimal"], "order_keys": [0], "null_marker": "\\N"}}
```

Column indices are zero-based. `text` is exact; `integer`, `decimal` and `float`
normalize numeric spelling without rounding or a global precision tolerance.
`order_keys` permits permutations only within consecutive equal-key groups;
the group sequence must match. `unordered: true` is reserved for queries with
no ordering contract and cannot be combined with `order_keys`. Without an
explicit contract the checker never guesses column types or discards order.
TSV capture must use an unambiguous NULL marker/escaping convention: legacy
files rendering both SQL NULL and the literal string `NULL` as `NULL` cannot
recover that distinction. The default treats that token as exact text.
For full type fidelity, use `capture_tabular_result.py --port PORT --database DB
--sql QUERY.sql --output q01.json` on each endpoint. Then select
`{"1": {"format": "typed-json"}}` in the comparison manifest and pass
`--actual-pattern 'q{query:02d}.json'`. Captures preserve column wire types,
SQL NULL, numeric-looking strings, exact decimals, binary bytes and temporal
values, and bind the evidence to the query's SHA-256. The checker rejects
changed metadata or query identities as well as changed values.

### Replication and oracle gates

`replication_check.py` waits for applied file/position (or durable applied LSN),
rejects replication errors and catchup timeouts, and compares every complete
typed row including duplicates. SQLite spills exact row multisets under
`/tmp/mydb` with bounded batch memory. The source must be quiescent during
verification; a changed source position fails the run. Shell drivers discover
tables from both endpoints instead of assuming workload-specific table names.
Use `MYDB_BINARY` to select an already-built server executable.

Snapshot initialization uses the dump-consistent file position. Combining
`setup_mysql_to_mydb_replica.py --snapshot` with `--auto-position` is rejected
before connecting or changing accounts: mydb's read-only GTID history cannot
be seeded from the dump. Incremental-only GTID setup remains available. Use `--mysqldump PATH` to
select a dump executable compatible with the source server (for example an
8.0 client for an 8.0 source). Dumps use hexadecimal binary values and stream
to disk; restore preserves executable SET/DDL comments, quoted/multiline data,
UTC timestamps and `NO_AUTO_VALUE_ON_ZERO` rather than dropping session state.
Unsupported DELIMITER/routine dumps and truncated statements fail explicitly.

Docker oracles create unique container names and owner labels. An explicitly
requested existing name is a collision, never a deletion target; cleanup checks
the captured container ID and owner label. TCP container readiness and SQL failures
fail the run. Custom data directories must be empty and below `/tmp/mydb`, and
are never removed by cleanup. PostgreSQL generation preserves an existing
golden file on query failure, reports failed/missing timing as `fail,-1`, and
returns nonzero if any selected query fails. Test-only paths can be redirected
with `TPCDS_PG_EXPECTED_DIR` and `TPCDS_PG_RESULT_DIR`.


Do not commit `__pycache__`, `.pyc`, `.DS_Store`, temporary data, logs, or local
performance reports. Internal tools may reuse public fixtures, but public builds
and default correctness regressions must work without `scripts-internal/`.

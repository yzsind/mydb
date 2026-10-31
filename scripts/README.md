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
Tool binaries require `--features tools`; Python drivers document their own
optional dependencies.

## Common entry points

| Purpose | Entry point |
|---|---|
| Portable Linux build and ABI contract | `build_linux_glibc217.py`, `check_elf_abi.py`, `verify_linux_compat.py` |
| Host-native release packaging and licenses | `package_release.py`, `release_licenses.py` |
| Windows checks and native isolated tests | `check_windows.py`, `test_windows_native.ps1` |
| Ubuntu development environment | `setup_ubuntu.sh` |
| Build and runtime-matrix command regressions | `test_release_build.py`, `test_linux_compat.py`, `test_elf_abi.py` |
| Native replication recovery and binary protocol | `test_native_readiness.py --binary target/debug/mydb` |
| Physical backup and WAL recovery | `test_backup_recovery.py --profile debug --rows 1000 --pairs 1`; add `--encrypted` for encrypted backups |
| Security, storage failure, atomic publication, and promotion | `test_release_security.py`, `test_release_storage.py`, `test_release_publication.py`, `test_release_promotion.py`; instrumentation sources remain in `fixtures/` |
| Replication consistency under load | `run_repl_tpcc_consistency.sh`, `run_mysql_repl_tpcc_consistency.sh`, `run_mydb_master_mysql_replica.sh`, `check_benchmarksql_consistency.sql` |
| Client tooling compatibility | `run_client_compat_matrix.sh`; see [client examples](../examples/README.md) |
| MySQL 8.0 capture, diff, and generated catalogs | `capture_mysql80_*.py`, `mysql80_*_diff.*`, `generate_mysql80_*.py` |
| Function and collation coverage | `check_function_test_coverage.py`, `generate_legacy_general_weights.py`, `generate_uca9_weights.py` |
| Function/expression performance comparison | `function_expression_bench.py`, `function_expression_report.py`; see [design and execution notes](../docs-internal/function-expression-benchmark-design.md) |
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

Query schemas, SQL files, and externally generated expected TSV files are
regression inputs. Rust parser and executor tests use some TPC-DS files through
`include_str!`; keep their public paths intact. Oracle generators and result
checkers also remain public. These inputs are community adaptations, not
TPC-audited performance results.

## Local checks

```bash
mkdir -p /tmp/mydb
PYTHONDONTWRITEBYTECODE=1 TMPDIR=/tmp/mydb python3 scripts/test_script_maintenance.py
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_release_build.py
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_linux_compat.py
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_elf_abi.py
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_function_expression_bench.py
python3 scripts/check_function_test_coverage.py --check
python3 scripts/generate_mysql80_information_schema_rust.py --check
python3 scripts/generate_mysql80_performance_schema_rust.py --check
scripts/render_storage_format_versions.sh --check
```

Do not commit `__pycache__`, `.pyc`, `.DS_Store`, temporary data, logs, or local
performance reports. Internal tools may reuse public fixtures, but public builds
and default correctness regressions must work without `scripts-internal/`.

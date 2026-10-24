# TPC-H Expected Results

Schemas, queries, result checkers, and oracle-generation tools remain public
regression inputs. Performance-driver examples below require the optional local
`scripts-internal/` directory, which is ignored by Git and absent from public
checkouts.

These directories contain PostgreSQL-generated canonical result sets for the
standard TPC-H Q1-Q22 SQL files in `scripts/tpch/queries`. They use data from
the official TPC-H `dbgen` binary and are selected by `scripts-internal/run_tpch.sh`
only when `TPCH_DATA_GENERATOR=dbgen` is active. Q23-Q26 are mydb workload
extensions and currently have no checked-in canonical fixtures; automatic
correctness mode reports them as skipped, while `TPCH_CHECK_CORRECTNESS=1`
keeps missing fixtures as a hard failure. DuckDB's generator produces
different random text fields, so its result fixtures must use a separate
`TPCH_EXPECTED_ROOT`.

- `sf-0.001`: scale factor 0.001
- `sf-0.01`: scale factor 0.01
- `sf-0.1`: scale factor 0.1
- `sf-1`: scale factor 1
- `sf-5`: scale factor 5

Files are tab-separated rows without column headers. Cells are trimmed, and
numeric values are canonicalized to integer form or 4 decimal places with
half-up rounding. `scripts-internal/run_tpch.sh` uses `scripts/tpch_check_results.py` to
apply the same canonicalization to mydb output before comparing it with these
fixtures.

Use `TPCH_CHECK_CORRECTNESS=0` or `scripts-internal/run_tpch.sh --no-check` to disable
the check. Use `TPCH_CHECK_CORRECTNESS=1` to require a checked-in expected set
for the requested scale factor.

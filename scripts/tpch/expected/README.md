# TPC-H Expected Results

Schemas, queries, result checkers, and oracle-generation tools remain public
regression inputs. Performance-driver examples below require the optional local
`scripts-internal/` directory, which is ignored by Git and absent from public
checkouts.

These directories contain PostgreSQL-generated canonical result sets for the
standard TPC-H Q1-Q22 SQL files in `scripts/tpch/queries`. They use data from
the official TPC-H `dbgen` binary and are selected by `scripts-internal/run_tpch.sh`
only when `TPCH_DATA_GENERATOR=dbgen` is active. Q23-Q26 are mydb workload
extensions with canonical fixtures at SF=10. At other scale factors, automatic
correctness mode reports missing fixtures as skipped, while
`TPCH_CHECK_CORRECTNESS=1` keeps missing fixtures as a hard failure. DuckDB's generator produces
different random text fields, so its result fixtures must use a separate
`TPCH_EXPECTED_ROOT`.

- `sf-0.001`: scale factor 0.001
- `sf-0.01`: scale factor 0.01
- `sf-0.1`: scale factor 0.1
- `sf-1`: scale factor 1
- `sf-5`: scale factor 5
- `sf-10`: scale factor 10, including Q23-Q26 extension queries. The adjacent
  manifest records input, SQL, schema, and expected-result SHA-256 hashes and
  the PostgreSQL oracle version.

Files are tab-separated rows without column headers. Cells are trimmed, and
numeric values are canonicalized to integer form or 4 decimal places with
half-up rounding. Each scale directory's `comparison.json` explicitly declares
the result column types, `trim_text: true`, `decimal_places: 4`, and the legacy
`NULL` marker. `scripts/tpch_check_results.py` automatically loads this adjacent
contract unless `--comparison-manifest` selects another file, so both standalone
checks and `scripts-internal/run_tpch.sh` apply the fixture's declared policy.
Row order, duplicate counts, integer values, and text spelling remain checked;
numeric-looking text columns are never converted to numbers. Fixtures without
an adjacent contract retain exact ordered TSV comparison.

These historical fixtures cannot prove numeric agreement beyond four decimal
places or preservation of leading/trailing text whitespace: that information
was discarded when they were generated. New references needing those checks
should preserve the original values and use an exact typed contract instead.

Use `TPCH_CHECK_CORRECTNESS=0` or `scripts-internal/run_tpch.sh --no-check` to disable
the check. Use `TPCH_CHECK_CORRECTNESS=1` to require a checked-in expected set
for the requested scale factor.

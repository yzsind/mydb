# TPC-DS benchmark suite for mydb

Schemas, queries, result checkers, and oracle-generation tools remain public
regression inputs. Performance-driver examples below require the optional local
`scripts-internal/` directory, which is ignored by Git and absent from public
checkouts.

A quick-to-run TPC-DS harness modelled on the TPC-H suite (`scripts-internal/run_tpch.sh`
+ `scripts/tpch/`). It generates data with the official TPC-DS toolkit, loads it
into mydb, builds join indexes, and runs the 99 TPC-DS queries adapted for
MySQL 8.0 / mydb.

## Layout

```
scripts-internal/run_tpcds.sh            driver: build → generate → load → index → analyze → 99 queries
scripts/tpcds_check_results.py  optional correctness checker (vs an expected result set)
scripts/tpcds/
  schema.sql       25 tables (24 benchmark + dbgen_version), column order = dsdgen output
  drop.sql         DROP TABLE for all tables
  indexes.sql      auxiliary secondary indexes on the star-join keys
  analyze.sql      ANALYZE TABLE for all 24 benchmark tables
  queries/q01..q99.sql   the 99 queries, adapted for mydb (see "Query adaptation")
  expected/        optional canonical result sets per scale factor (see expected/README.md)
```

## Prerequisites: the TPC-DS toolkit (dsdgen / dsqgen)

Data and queries come from the TPC-DS toolkit. A buildable mirror is
`gregrahn/tpcds-kit`. On macOS the K&R-era sources need a few warnings demoted:

```bash
cd ~/prog
git clone --depth 1 https://github.com/gregrahn/tpcds-kit.git
cd tpcds-kit/tools
make OS=MACOS MACOS_CFLAGS="-g -Wno-implicit-int -Wno-implicit-function-declaration -Wno-int-conversion -Wno-deprecated-non-prototype -Wno-error"
# produces: dsdgen, dsqgen, tpcds.idx
```

`run_tpcds.sh` defaults to the sibling `../tpcds-kit/tools` directory. Point
it elsewhere with `TPCDS_DSDGEN=/path/to/tpcds-kit/tools`.

The 99 query files are already generated and checked in, so dsqgen is only needed
if you want to regenerate them (see "Regenerating the queries").

## Quick start

```bash
# Full run at SF=1 (~1 GB): generate (cached) → load → index → analyze → 99 queries
TPCDS_SKIP_BUILD=1 scripts-internal/run_tpcds.sh

# Load once, then iterate on queries without reloading:
TPCDS_SKIP_BUILD=1 scripts-internal/run_tpcds.sh --load-only
TPCDS_SKIP_BUILD=1 scripts-internal/run_tpcds.sh --queries-only

# A subset of queries:
TPCDS_SKIP_BUILD=1 TPCDS_QUERIES="1 5 22 67" scripts-internal/run_tpcds.sh --queries-only
```

Results land in `/tmp/mydb/tpcds-results/<timestamp>/` (`query_times.csv`,
`index_times.csv`, per-query `qNN_result.txt`/`qNN_error.txt`, `server.log`,
`mydb_status.txt`, `summary.txt`).

## Options and key environment variables

| Flag / env | Meaning |
|---|---|
| `--load-only` / `TPCDS_LOAD_ONLY=1` | generate + load + index + analyze, no queries |
| `--queries-only` / `TPCDS_QUERIES_ONLY=1` | run queries only (reuse loaded `MYDB_DATA_DIR`) |
| `--skip-build` / `TPCDS_SKIP_BUILD=1` | reuse `target/<profile>/mydb` |
| `--keep-server` / `TPCDS_KEEP_SERVER=1` | leave the server running on exit |
| `--no-check` / `TPCDS_CHECK_CORRECTNESS=0` | disable the correctness check |
| `TPCDS_DSDGEN` | toolkit dir with `dsdgen` + `tpcds.idx` |
| `TPCDS_SCALE_FACTOR` | scale in GB (default `1`) |
| `TPCDS_QUERIES` | subset, e.g. `"1 6 14"` (default `1..99`) |
| `TPCDS_FORCE_DSDGEN=1` | regenerate the cached data |
| `MYDB_DATA_DIR` | mydb data dir (default `/tmp/mydb/tpcds-data`) |
| `MYDB_PORT` / `MYDB_HOST` | default `3307` / `127.0.0.1` |
| `MYDB_BUFFER_POOL_PAGES` / `MYDB_MEMORY_TARGET` | buffer pool sizing (default 131072 pages = 2 GiB) |
| `MYDB_LOAD_DURABILITY` / `MYDB_QUERY_DURABILITY` | default `strict`; use `relaxed` for faster non-perf runs |

Generated `.dat` files are cached per scale factor under
`/tmp/mydb/tpcds-dbgen-cache/sf-<SF>` (override with `TPCDS_DSDGEN_CACHE_ROOT`).

## Data and NULL handling

`dsdgen` writes one `|`-separated `.dat` per table and emits an **empty field for
NULL**. mydb's `LOAD DATA` does not support the `(@v) SET col=NULLIF(@v,'')`
form, so at cache-build time the driver rewrites every empty field to the
`LOAD DATA` NULL marker `\N` using `scripts-internal/lib/nullify_empty_fields.py`
(Python standard library only, cached). The converter applies literal regex
replacements to 2 MiB byte blocks and processes independent tables concurrently
with four worker processes (`TPCDS_NULLIFY_WORKERS` overrides this; `1` runs
serially). Larger tables are scheduled first. It preserves values as bytes and
handles consecutive and leading/trailing empty fields across buffer boundaries.
Sub-1 scales retain `ceil(row_count * fraction)` fact rows; dimensions stay full.
Positional
`LOAD DATA ... FIELDS TERMINATED BY '|'` then stores real NULLs. Data is
generated with `-terminate n` (no trailing delimiter) so the field count matches
`schema.sql` exactly — no column list needed.

`dbgen_version` is created by `schema.sql` for fidelity with the official
`tpcds.sql` but is not loaded or queried (it is a generator artifact).

## Query adaptation (tpcds-kit → mydb / MySQL 8.0)

The query files are generated from the kit's templates with the `netezza`
dialect (its `LIMIT n` form matches MySQL) and then transformed:

- **Date arithmetic** `cast('…' as date) + N days` → `… + interval N day`.
- **ROLLUP** `group by rollup(a, b, …)` → `group by a, b, … with mydb's
  `with rollup` form (mydb intentionally does not support `rollup(expr)`).
- **String concat `||`** is left as-is; queries run with
  `sql_mode = PIPES_AS_CONCAT` (set on the query connection via the mysql
  client's `--init-command`), so `a || b` behaves as `CONCAT(a, b)`.

Constructs used as-is because mydb supports them: `INTERSECT` / `EXCEPT`,
`GROUPING()`, `STDDEV*`, CTEs, window functions.

## Correctness checking (optional)

If `scripts/tpcds/expected/sf-<SF>/qNN.tsv` files exist, the driver compares mydb
output against them with the same canonicalization as the TPC-H checker
(trim + numeric rounding to 4 dp). None are checked in by default (TPC-DS answer
sets depend on the substitution parameters / RNG seed). See
`scripts/tpcds/expected/README.md` to generate one from a reference database.
Without an expected set the check auto-skips; the run still reports
OK/FAIL/SKIP per query (i.e. whether each query *executed*).

Two reference sets are checked in, generated from the *same* cached `.dat` files
mydb loads and the *same* query files mydb runs:

- `expected/` — **PostgreSQL 18** oracle (`pg/build_and_run_pg.sh`); auto-picked
  by `run_tpcds.sh`.
- `expected-duckdb/` — **DuckDB** oracle (`duckdb/build_and_run_duckdb.sh`).
  DuckDB's `/` is decimal division like MySQL/mydb (PG truncates `int/int`), so
  it is the more faithful reference for the integer-ratio queries. See
  `expected-duckdb/README.md` for the per-query DuckDB-vs-PG cross-check and how
  to point the checker at it.

## Regenerating the queries

The checked-in queries use `dsqgen -rngseed 1 -scale 1`. To regenerate:

```bash
cd ~/prog/tpcds-kit/tools
for n in $(seq 1 99); do
  ./dsqgen -template query${n}.tpl -directory ../query_templates \
           -dialect netezza -scale 1 -output_dir /tmp/dsq -quiet y -rngseed 1
  # then apply the date/rollup transforms above
done
```

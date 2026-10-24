# TPC-DS expected result sets

Schemas, queries, result checkers, and oracle-generation tools remain public
regression inputs. Performance-driver examples below require the optional local
`scripts-internal/` directory, which is ignored by Git and absent from public
checkouts.

This directory holds canonical query results, one tab-separated file per query
(`sf-<SF>/qNN.tsv`, no header), used by `scripts/tpcds_check_results.py` to
validate mydb output. The driver (`scripts-internal/run_tpcds.sh`) applies the same
canonicalization to both sides: cells trimmed; numbers normalized to integer
form or 4 decimals (half-up); `NULL` printed as the literal token; rows sorted
lexicographically after cell normalization. Sorting preserves duplicates while
avoiding false failures when two engines emit ORDER BY ties in different order.

TPC-DS results depend on the substitution parameters baked into the query files
(here: `dsqgen -rngseed 1 -scale 1`), so an expected set is only meaningful when
produced from the *same* query files. The checked-in set under `sf-1/` was
generated from a reference PostgreSQL 18 instance running the *exact* mydb query
files (transformed only for PG dialect — see below), so it corresponds to the
SQL mydb runs. `run_tpcds.sh` picks it up automatically
(`TPCDS_CHECK_CORRECTNESS=auto`); require it with `TPCDS_CHECK_CORRECTNESS=1`.

## What is checked in

Two scale factors, each `q01.tsv … q99.tsv` (tab-separated, no header,
NULL = literal `NULL`, matching mydb's `mysql --batch --skip-column-names`) plus
a `REPORT.md` (row counts, PG warm `parallel=2` timings, per-query status):

- **`sf-1/`** — full SF=1 dataset (`run_tpcds.sh` default). All 99 queries
  return PG results.
- **`sf-0.1/`** — the **subsampled** SF=0.1 dataset `run_tpcds.sh` produces for
  `TPCDS_SCALE_FACTOR=0.1`: fact tables truncated to 10%, all 17 dimension
  tables kept full, queried with the *same* scale-1 substitution parameters. The
  10% fact slice makes several queries return few or zero rows — that is the
  correct answer *for this data*, not an error. (There is no TPC-DS spec answer
  set for a subsampled dataset; this PG run is the reference.)

Both were produced from the local PG 18 (`127.0.0.1:15432`). Regenerate either
end to end (create db → load the SAME cached `.dat` files mydb uses → indexes →
ANALYZE → golden pass + perf pass → `REPORT.md`) with one command:

```bash
scripts/tpcds/pg/build_and_run_pg.sh 1      # -> sf-1/   (db tpcds_sf1)
scripts/tpcds/pg/build_and_run_pg.sh 0.1    # -> sf-0.1/ (db tpcds_sf0_1)
```

It calls `gen_pg_artifacts.py` (PG-dialect schema/indexes/queries),
`run_pg_expected.sh` (the two passes), and `summarize_pg_results.py` (REPORT.md).

## PG dialect transforms (`scripts/tpcds/pg/gen_pg_artifacts.py`)

The mydb queries are MySQL 8.0 dialect; the generator derives PG equivalents
with only semantics-preserving rewrites, so the answers are unchanged:

- `interval N day` → `interval 'N day'` (PG requires the quotes).
- `group by … with rollup` → `group by rollup(…)` (incl. multi-line and nested).
- `case when lochierarchy = N …` (an ORDER-BY alias used *inside* an expression,
  which PG does not resolve) → the alias's `grouping(a)+grouping(b)` definition.
- `||` concat and everything else are standard SQL PG runs natively.

## Known cross-engine divergences (read before trusting a mismatch)

The checker compares canonical row **multisets**. These PG-vs-mydb semantic
differences can still matter before canonicalization:

- **NULL ordering** *(neutralized in the harness — see below).* PG sorts `NULL`
  **last** under `ORDER BY … ASC`; MySQL/mydb sorts it **first**. This reorders
  (and, under `LIMIT`, can change which rows appear in) the 11 `ROLLUP` queries
  whose super-aggregate rows carry `NULL` grouping keys: **q05 q14 q18 q22 q27
  q36 q67 q70 q77 q80 q86**. `run_tpcds.sh` now starts the server with
  `MYDB_ORDER_BY_NULLS=last` (knob: `TPCDS_ORDER_BY_NULLS`, default `last`) so
  mydb adopts PG-style placement and these no longer diff. Set
  `TPCDS_ORDER_BY_NULLS=first` to restore MySQL semantics.
- **Tie-breaking.** Where `ORDER BY` is not a total order, ties break by each
  engine's scan/join order. The checker intentionally neutralizes order-only
  differences between otherwise identical tied rows.
- **Integer division.** PG `int / int` truncates; MySQL `/` yields a decimal.
  Queries computing ratios over integer columns may differ in the fraction.

For perfectly comparable `LIMIT` runs, use a total `ORDER BY`: sorting output
cannot neutralize two engines selecting different rows at a tied LIMIT boundary.

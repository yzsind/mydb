# ClickBench Result Correctness Checking

Schemas, queries, result checkers, and oracle-generation tools remain public
regression inputs. Performance-driver examples below require the optional local
`scripts-internal/` directory, which is ignored by Git and absent from public
checkouts.

After the timed queries finish, `run_clickbench.sh` compares every round of each
selected query against the full DuckDB result cache and writes
`RESULT_DIR/correctness.csv`. Time spent checking and generating baselines is
not counted in `Total query execution time`. `--load-only` and
`--download-only` do not check query results.

## Usage

On a first run with no baseline cache, use the one-shot preparation script
(local machine only):

```bash
scripts/prepare_clickbench_expected.sh
scripts-internal/run_clickbench.sh --queries-only --check
```

The preparation script reuses `/tmp/mydb/clickbench-cache/hits.tsv.gz`; when
missing, it downloads the official file and verifies size and SHA256 before
importing. It reads the gzip stream directly without generating extra TSV
files, verifies the 99,997,497 rows after import, keeps
`CACHE_DIR/duckdb/hits-v2.duckdb` plus the manifest, and then generates all 43
`expected-duckdb/qNN.json` files. On later runs a complete valid result cache
is reused directly; with partial cache misses the finished DuckDB import is
reused and generation continues. Failed imports leave only `.loading` staging
files and are never treated as a complete database. Existing databases without
a matching manifest are not overwritten.

Defaults are 4 threads and an 8 GB DuckDB memory limit, adjustable via
`--threads` / `--memory-limit` or `CLICKBENCH_DUCKDB_THREADS` /
`CLICKBENCH_DUCKDB_MEMORY_LIMIT`. See
`scripts/prepare_clickbench_expected.sh --help` for the remaining options.
Import logs are written to `CACHE_DIR/duckdb-load.log`. Custom data requires
`--archive-bytes`, `--archive-sha256`, `--row-count`, and `--dataset-id`, and
the runner must use the same dataset identifier. With an existing external
DuckDB database, use `CLICKBENCH_DUCKDB_DATABASE` below or the standalone
`generate` command instead of the preparation script's manifest.

The default is `CLICKBENCH_CHECK_CORRECTNESS=auto`: existing cache entries are
checked, missing entries are reported explicitly as `skipped_missing_expected`,
and result mismatches, corrupted/stale caches, and query execution errors all
fail the run.

```bash
# Require every round of every selected query to pass; a missing baseline also fails.
scripts-internal/run_clickbench.sh --queries-only --check

# First generate the missing cache entries from an existing DuckDB hits table,
# then verify. That database must already hold the same data as mydb; the
# script only reads it.
CLICKBENCH_DUCKDB_DATABASE=/path/to/hits.duckdb \
  scripts-internal/run_clickbench.sh --queries-only --check

# Read-only cache reuse when results match exactly; a live DuckDB is needed
# when LIMIT legitimately returns a different set of rows.
scripts-internal/run_clickbench.sh --queries-only --check --queries 1,3-5 --runs 3

# Explicitly disable checking.
scripts-internal/run_clickbench.sh --no-check
```

The cache lives at
`${CLICKBENCH_CACHE_DIR:-/tmp/mydb/clickbench-cache}/expected-duckdb` by
default and can be overridden with `CLICKBENCH_EXPECTED_DIR`. Each `qNN.json`
stores the full rows, column types, MySQL/DuckDB SQL, the DuckDB version, cache
checksums, and workload/schema/dataset metadata. Valid caches are reused
directly; `CACHE_DIR/duckdb/hits-v2.duckdb` is discovered automatically by
default for tie verification, and stale entries are regenerated when
`CLICKBENCH_DUCKDB_DATABASE` is provided. `DUCKDB_BIN` selects the CLI, and
`CLICKBENCH_DUCKDB_TIMEOUT` bounds each baseline command (default 1800
seconds). A failed generation never publishes an incomplete cache, and old
stale caches are never treated as valid results.

The cache can also be prepared standalone, without starting mydb:

```bash
python3 scripts/clickbench_check_results.py generate \
  --duckdb-database /path/to/hits.duckdb \
  --expected-dir /tmp/mydb/clickbench-cache/expected-duckdb \
  --queries-file /tmp/mydb/clickbench-cache/queries.sql \
  --schema-file scripts/clickbench/create.sql \
  --dataset-id 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz#bytes=16298506510' \
  --row-count 99997497
```

`--queries-file` should be the pinned query file cached by the runner. The
generator produces all 43 queries by default and also supports
`--queries 1,3,5`. It never downloads or re-imports the whole DuckDB database
automatically.

## Unified comparison semantics and legacy data migration

From format v2 on, table creation explicitly uses `utf8mb4_0900_bin`
(case-sensitive, accent-sensitive, no trailing-space padding) and mydb LOAD
DATA uses `ESCAPED BY ''`, matching DuckDB's raw TSV reading; NULL is marked
as the full field `NULL` on both sides. Import and query connections are
pinned to UTC, while MySQL SQL strings still use default backslash
interpretation. This is only the cross-engine benchmark configuration — it
does not change mydb's default MySQL collation or import semantics. The 43
timed SQL statements are unchanged. **Timings under the new semantics must not
be compared directly with the old ai_ci / escaped-import timings.**

Older mydb data may have lost backslashes and been indexed under the old
collation; changing only the query session cannot repair that. The runner
validates the import contract through `.clickbench-load.json` and fails
explicitly on mismatch instead of overwriting existing tables. Use a fresh
`MYDB_DATA_DIR`, or explicitly re-import with the commands below (this
rebuilds the test database):

```bash
scripts/prepare_clickbench_expected.sh
MYDB_MEMORY_TARGET=90G MYDB_PARALLEL_DEGREE=16 scripts-internal/run_clickbench.sh --load-only
MYDB_MEMORY_TARGET=90G MYDB_PARALLEL_DEGREE=16 scripts-internal/run_clickbench.sh --queries-only --check
```

The new DuckDB default path is `duckdb/hits-v2.duckdb`; the old database is
kept, and the result cache is regenerated after the format upgrade. The import
identity also binds the schema, dataset, and database name. Custom DuckDB
paths require setting `CLICKBENCH_DUCKDB_DATABASE` for the runner as well.
Consistency of the underlying data on both sides remains the caller's
responsibility.

## Verification boundaries

- The dataset identity defaults to the download URL and the expected archive
  size, and DuckDB/mydb row counts are checked for equality. This is not a
  hash proof of content: callers must ensure both databases hold the same
  data; when using custom data or refreshing an existing table, set a new
  `CLICKBENCH_DATASET_ID` (a content SHA256 is recommended).
- The cache binds the full query set, the create-table file, the dataset
  identity, and the row count. Regenerate after any change; never mix.
- mydb query results use MySQL **escaped batch TSV** — no longer `--raw` — to
  preserve tabs, newlines, NUL, and backslashes inside text. Old raw results
  cannot be fed to this checker directly.
- Integers and DECIMALs compare exactly; text is not trimmed and
  numeric-looking strings are not converted to numbers. DuckDB
  floating-point columns use a `max(0.00005, abs(expected) * 1e-12)`
  tolerance to accommodate MySQL AVG's four-decimal output.
- Without ORDER BY, rows are compared as multisets with duplicate counts;
  when the sort columns can be parsed, the sort-key sequence is verified and
  full rows may be swapped within the same sort key. Other ordered queries
  compare in result order.
- LIMIT-boundary ties, LIMIT without ORDER BY, and cases where sort keys are
  not projected can legitimately produce different rows in the two engines. A
  finite result cache alone cannot prove an alternate set of rows correct;
  the checker reports `mismatch` and asks for further verification against a
  live baseline database. With a live DuckDB, RANK and tie-group sizes are
  computed from the full results to verify the values, the legal position
  range, and the duplicate count of the returned rows (including OFFSET).
  Candidate-row filtering happens after the window computation, so wrongly
  low-ranked rows cannot be filtered into first place. Verification time is
  not counted in query timings.
- Both sides consistently use binary collation, raw TSV, and UTC. MySQL
  connections set an empty sql_mode explicitly. The adapter translates LENGTH
  to DuckDB's byte-length STRLEN, parses backslashes in SQL literals per the
  MySQL default mode, and makes REGEXP_REPLACE replacement global. The `\1`
  in the original Q29 is actually the literal `1`; DuckDB's capture-group
  reference must not be treated as its expected value. Q43 keeps **hour**
  grouping.
- NULL and the text literal `NULL` are ambiguous in MySQL batch output; a
  benchmark containing the latter refuses to confirm a pass and needs typed
  result collection. Empty strings and empty result sets remain distinct.
- Terminal tables from historical DuckDB reports may have truncated fields
  and cannot serve as a complete cache; regenerate the JSON files above from
  the original DuckDB database.

To verify the scripts themselves:

```bash
python3 -m unittest discover -s scripts -p 'test*clickbench*.py'
bash -n scripts-internal/run_clickbench.sh
```

The tests cover cache hits/invalidation, all rounds, the missing-baseline
policy, numeric precision, text escaping, duplicate rows, sort ties, failure
exits, and runner integration. With the DuckDB CLI available, real generation
and reuse tests also run against a small temporary local database.

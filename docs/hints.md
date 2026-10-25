# HINT Syntax and Behavior

This document describes the query hints supported by mydb, including
conflict handling, diagnostics, and their effects on query execution.
"Supported" in this document falls into two categories:

- **Effective**: the hint participates in choosing access paths, join
  algorithms, degree of parallelism, query timeout, or vector search
  parameters.
- **Accepted for compatibility**: the syntax parses, but the current
  execution path does not use the hint; such behavior is explicitly called
  out in this document, and applications should not rely on it to change
  execution plans.

## 1. Support Overview

mydb supports two hint syntaxes:

1. MySQL table-level index hints, written after the table reference:

   ```sql
   SELECT *
   FROM orders FORCE INDEX FOR JOIN (idx_customer)
   WHERE customer_id = 100;
   ```

2. `/*+ ... */` optimizer hints, placed immediately after the statement
   keyword:

   ```sql
   SELECT /*+ ORDERED HASH_JOIN(o, c) PARALLEL(4) */
          o.id, c.name
   FROM orders AS o
   JOIN customers AS c ON c.id = o.customer_id;
   ```

The currently effective `/*+ ... */` hints include:

| Category | Hints |
|---|---|
| Scan/Index | `INDEX`/`USE_INDEX`, `NO_INDEX`, `INDEX_MERGE`, `NO_INDEX_MERGE`, `FULL_SCAN`, `INDEX_FFS` |
| Zone Map | `ZONE_MAP`, `NO_ZONE_MAP` |
| Join | `HASH_JOIN`, `NO_HASH_JOIN`, `NL_JOIN`, `INDEX_NLJ`, `ORDERED` |
| Parallelism | `PARALLEL`, `NO_PARALLEL` |
| Runtime | `MAX_EXECUTION_TIME`, `SET_VAR(select_into_buffer_size=...)`, `SET_VAR(select_into_disk_sync=...)`, `SET_VAR(select_into_disk_sync_delay=...)` |
| Vector Search | `HNSW_EF`, `IVF_NPROBE`, `NO_VECTOR_INDEX` |

## 2. General Rules

### 2.1 Placement

Optimizer hints for `SELECT` must immediately follow `SELECT`:

```sql
SELECT /*+ FULL_SCAN(t) */ * FROM t;
```

For `EXPLAIN`, the hint still goes after the `SELECT` being explained:

```sql
EXPLAIN FORMAT=TREE
SELECT /*+ USE_INDEX(t idx_status) */ *
FROM task AS t
WHERE status = 'ready';
```

`LOAD DATA` / `LOAD XML` consume only parallelism hints; the hint
immediately follows `DATA` or `XML`:

```sql
LOAD DATA /*+ PARALLEL(4) */
INFILE '/tmp/data.csv'
INTO TABLE t;

LOAD XML /*+ NO_PARALLEL */
INFILE '/tmp/data.xml'
INTO TABLE t;
```

### 2.2 Case Sensitivity, Delimiters, and Multiple Hints

- Hint names are case-insensitive.
- Multiple hints may be written inside the same `/*+ ... */`, separated by
  whitespace.
- Parameters may be separated by commas or whitespace; the following two
  spellings are equivalent:

  ```sql
  /*+ USE_INDEX(t, idx_a, idx_b) */
  /*+ USE_INDEX(t idx_a idx_b) */
  ```

- Table and index name matching is case-insensitive. Table parameters may
  use the alias from the query; when an alias exists, always using the
  alias is recommended.
- The optimizer hint parameter parser targets simple identifiers and
  integers. To avoid ambiguity, do not use quoted identifiers containing
  spaces, commas, or parentheses in `/*+ ... */` parameters.
- Only the first hint in a conflict group is kept; later duplicates or
  conflicting entries produce a
  `duplicate or conflicting hint ignored (first wins)` warning. Scan
  access is grouped per table, Zone Map per table, join algorithms per
  sorted set of tables, and parallelism uses a single statement-wide
  group. Parallelism is the exception to first-wins: `NO_PARALLEL` wins over
  `PARALLEL` in either order.

### 2.3 Hints Are Not Correctness Constraints

Except for `MAX_EXECUTION_TIME`, hints only affect execution strategy and
do not change query results. When the preconditions for a physical path
do not hold, mydb falls back to an executable regular plan. For example:

- `HASH_JOIN` cannot build a hash join when the join has no extractable
  equi-join keys;
- `INDEX_NLJ` cannot build an index nested-loop join when the inner table
  has no usable join index;
- `INDEX_FFS` falls back to a regular access path when the specified index
  does not cover the columns required by the query;
- `PARALLEL` still runs with a serial plan when there is no parallelizable
  operator or the effective degree of parallelism is 1.

Use `EXPLAIN` or `EXPLAIN FORMAT=TREE` to verify whether a hint actually
made it into the plan.

## 3. Table-Level Index Hints

### 3.1 Syntax

```text
table_name [AS alias]
  { USE | FORCE | IGNORE } { INDEX | KEY }
  [ FOR { JOIN | ORDER BY | GROUP BY } ]
  (index_name [, index_name] ...)
```

`INDEX` and `KEY` are synonyms. Multiple index hints may be written one
after another after a table reference:

```sql
SELECT *
FROM t
     USE INDEX FOR JOIN (idx_a, idx_b)
     IGNORE INDEX FOR ORDER BY (idx_b)
WHERE a = 10
ORDER BY b;
```

### 3.2 Type Semantics

| Type | Behavior |
|---|---|
| `USE INDEX` | Restricts the candidate indexes within the applicable scope to the given set, but the CBO may still compare costs between index paths and a sequential scan. |
| `FORCE INDEX` | Restricts candidate indexes to the given set; when a usable named-index path exists, the final sequential-scan cost fallback is skipped. |
| `IGNORE INDEX` | Excludes the given indexes from the applicable scope. |

Scope meanings:

| Scope | Effect |
|---|---|
| Omitting `FOR` | Applies to all three index selection categories: JOIN, ORDER BY, and GROUP BY. |
| `FOR JOIN` | Affects predicate access paths and inner-table index selection for joins. |
| `FOR ORDER BY` | Affects the choice of indexes used to preserve `ORDER BY` order. |
| `FOR GROUP BY` | Affects the choice of indexes used for streaming/ordered grouping. |

Within the same scope, `IGNORE INDEX` exclusions take precedence over the
`USE/FORCE INDEX` allow set. An empty index list parses, but the current
implementation does not turn it into a candidate restriction; do not use
`USE INDEX ()` to mean "disallow all indexes" — use `FULL_SCAN(table)`
instead.

### 3.3 Examples

```sql
-- Let the CBO choose between the two indexes
SELECT * FROM orders USE INDEX (idx_customer, idx_created)
WHERE customer_id = 100;

-- Keep the named range index path when available
SELECT * FROM orders FORCE KEY (idx_created)
WHERE created_at >= '2026-01-01';

-- Exclude one index but allow the others
SELECT * FROM orders IGNORE INDEX (idx_status)
WHERE status = 'ready';

-- Constrain only index selection for ORDER BY
SELECT * FROM orders USE INDEX FOR ORDER BY (idx_created)
ORDER BY created_at LIMIT 20;
```

The execution semantics of table-level index hints currently follow
`SELECT`. Even when DML table references are accepted syntactically, the
corresponding execution paths do not guarantee that these hints are
consumed.

## 4. `/*+ ... */` Scan and Index Hints

### 4.1 `INDEX(table, index...)` / `USE_INDEX(table, index...)`

Restricts the candidate indexes of the target table to one or more named
indexes; unlike the table-level `USE INDEX`, it additionally prevents the
final CBO sequential-scan cost fallback whenever a usable named-index
access path exists.

```sql
SELECT /*+ USE_INDEX(o idx_customer idx_created) */ *
FROM orders AS o
WHERE o.customer_id = 100;
```

At least one table parameter and one index parameter are required. A
nonexistent table or index produces a hint warning. `INDEX` is a synonym
spelling of `USE_INDEX`; both have identical plan semantics.

### 4.2 `NO_INDEX(table, index)`

Excludes one index from the target table's candidates. It strictly takes
two parameters.

```sql
SELECT /*+ NO_INDEX(o idx_a) */ *
FROM orders AS o
WHERE o.customer_id = 100;
```

Note: `INDEX/USE_INDEX`, `NO_INDEX`, `INDEX_MERGE`, `NO_INDEX_MERGE`,
`FULL_SCAN`, and `INDEX_FFS` on the same table belong to one scan-access
conflict group, and the first one wins. Consequently, you cannot exclude
multiple indexes of the same table by repeating `NO_INDEX`, nor should you
rely on `NO_INDEX` to override an earlier `USE_INDEX`. To allow a set of
multiple indexes, use a single `USE_INDEX(table, index...)`; to disable
index access entirely, use `FULL_SCAN(table)`.

### 4.3 `INDEX_MERGE` / `NO_INDEX_MERGE`

Forces or disables same-table multi-index RowId merging; the index list
may be omitted to mean all applicable combinations:

```sql
SELECT /*+ INDEX_MERGE(t idx_a idx_b) */ *
FROM t WHERE a = 1 OR b = 2;

SELECT /*+ NO_INDEX_MERGE(t) */ *
FROM t WHERE a = 1 OR b = 2;
```

`INDEX_MERGE` can override the session
`optimizer_switch='index_merge=off'`; every named index must be able to
participate in the current boolean predicates, otherwise a warning is
produced and the regular CBO takes over again. `NO_INDEX_MERGE(t idx_a)`
excludes every combination containing `idx_a`.

### 4.4 `FULL_SCAN(table)`

Forces the target table onto a heap sequential scan and prevents later
rewrites such as covering-index aggregation from reintroducing index
paths:

```sql
SELECT /*+ FULL_SCAN(o) */ COUNT(*)
FROM orders AS o;
```

Exactly one table parameter must be provided; extra parameters cause the
hint to be ignored with a warning.

### 4.5 `INDEX_FFS(table [index])`

Requests an Oracle-style index fast full scan: scan the covering index
directly in full without fetching heap values. Heap MVCC headers may still be
read when the page-level visibility shortcut cannot prove visibility.

```sql
-- Automatically choose the narrowest covering index
SELECT /*+ INDEX_FFS(o) */ customer_id, created_at
FROM orders AS o;

-- Specify a covering index
SELECT /*+ INDEX_FFS(o idx_customer_created) */ customer_id, created_at
FROM orders AS o;
```

Preconditions:

- The index must cover the projection columns and the columns required by
  the filter conditions;
- Indexes containing prefix columns that cannot fully supply the required
  values are not used;
- When no index name is given, the covering index with the fewest
  key/effective columns is selected;
- When the specified index does not exist or cannot cover the query, the
  hint does not fabricate an invalid plan but falls back to regular CBO
  selection. A nonexistent index produces a hint warning; an index that
  exists but cannot cover produces no extra warning, so check whether an
  `INDEX_FFS` node/note appears in `EXPLAIN` to determine whether it was
  applied.

### 4.6 `ZONE_MAP(table)` / `NO_ZONE_MAP(table)`

`ZONE_MAP` requests Zone Map skip scanning for the target table;
`NO_ZONE_MAP` disables Zone Map access for the table:

```sql
SELECT /*+ ZONE_MAP(e) */ * FROM events AS e
WHERE e.ts >= '2026-08-01';

SELECT /*+ NO_ZONE_MAP(e) */ * FROM events AS e
WHERE e.ts >= '2026-08-01';
```

Both take exactly one table parameter, and the first one wins for the same
table. `ZONE_MAP` can override the session
`optimizer_switch='zone_map=off'` but cannot bypass a table-level
`ZONE_MAP=OFF`; it warns/falls back when the table does not have Zone Map
enabled, when the predicates do not support safe pruning, or when the
metadata is untrusted. The executor always retains the original row
predicates as a correctness check.

## 5. Join Hints

### 5.1 `ORDERED`

Preserves the join order as written in `FROM` / `JOIN`, skipping the
DP/CBO join reordering of inner joins:

```sql
SELECT /*+ ORDERED */ *
FROM fact AS f
JOIN dim_a AS a ON a.id = f.a_id
JOIN dim_b AS b ON b.id = f.b_id;
```

`ORDERED` fixes only the join order, not the join algorithm of each step;
it can be combined with the algorithm hints below.

### 5.2 `HASH_JOIN(table...)`

Forces hash join for applicable joins that contain all the listed tables,
provided the join can extract equi-join keys:

```sql
SELECT /*+ ORDERED HASH_JOIN(f, d) */ *
FROM fact AS f
JOIN dim AS d ON d.id = f.dim_id;
```

### 5.3 `NO_HASH_JOIN(table...)`

Excludes hash join for matching joins, letting the CBO choose among
candidates such as index nested-loop and plain nested-loop joins:

```sql
SELECT /*+ NO_HASH_JOIN(o, c) */ *
FROM orders AS o
JOIN customers AS c ON c.id = o.customer_id;
```

### 5.4 `NL_JOIN(table...)`

Forces a plain nested-loop join for matching joins:

```sql
SELECT /*+ NL_JOIN(a, b) */ *
FROM a JOIN b ON a.k > b.k;
```

### 5.5 `INDEX_NLJ(table...)`

Requests an index nested-loop join. The hint must cover the inner table of
the current join (the right side of the plan tree), and the inner table
must have an index that can serve the join condition:

```sql
SELECT /*+ ORDERED INDEX_NLJ(o, l) */ o.id, l.sku
FROM orders AS o
JOIN order_lines AS l ON l.order_id = o.id;
```

Only the inner table may be listed:

```sql
SELECT /*+ ORDERED INDEX_NLJ(l) */ o.id, l.sku
FROM orders AS o
JOIN order_lines AS l ON l.order_id = o.id;
```

### 5.6 Matching and Conflict Rules

- `table...` takes at least one parameter; table names or aliases may be
  used.
- A hint matches a join step only when that step contains every table the
  hint lists; in multi-table queries it is therefore recommended to
  combine it with `ORDERED` to make explicit which join step sees the
  table set.
- Join algorithm hints are looked up in the order they are written in the
  comment, and the current join uses the first matching hint.
- When `HASH_JOIN` and `NO_HASH_JOIN` appear for the same table set,
  `EXPLAIN` reports the conflict; the actual algorithm is still governed
  by the "first matching hint" rule. Do not rely on the ordering outcome
  of conflicting hints.
- When the physical preconditions of a forced algorithm do not hold,
  execution falls back safely. For example, a hash join cannot run
  without equi-keys, and an index nested-loop join cannot run without a
  usable inner-table index.

## 6. Parallelism Hints

### 6.1 `PARALLEL[(degree)]`

Requests parallel execution:

```sql
SELECT /*+ PARALLEL(4) */ customer_id, COUNT(*)
FROM orders
GROUP BY customer_id;
```

The following spellings all request the engine default degree of
parallelism:

```sql
SELECT /*+ PARALLEL */ * FROM t;
SELECT /*+ PARALLEL() */ * FROM t;
SELECT /*+ PARALLEL(0) */ * FROM t;
```

The query-side default degree of parallelism comes from
`MYDB_PARALLEL_DEGREE`, which is 4 when unset. The requested value is
still constrained by operator applicability, ordering-safety checks, the
CPU worker cap, and forced-serial scopes. Parallelizable objects include
qualified scans, hash aggregates, sorts, and other plan nodes.

`PARALLEL` and `NO_PARALLEL` in the same statement share one conflict
group; `NO_PARALLEL` always wins, and conflicting entries produce a hint
warning. Extra `PARALLEL` parameters likewise produce a warning.

### 6.2 `NO_PARALLEL`

Forces serial execution and suppresses CBO auto-parallelism:

```sql
SELECT /*+ NO_PARALLEL */ * FROM t;
```

Written as `/*+ PARALLEL(8) NO_PARALLEL */`, execution is still forced
serial and a conflict warning is reported.

In addition, the process environment variable
`MYDB_PARALLEL_DISABLE=1|true|on` suppresses `PARALLEL` requests on
`SELECT`. `LOAD DATA/XML` also supports `PARALLEL` and `NO_PARALLEL`;
its small-file detection may still choose the synchronous path.

### 6.3 B-tree Index Creation

Offline B-tree builds accept a hint written after the first keyword of
the DDL:

```sql
CREATE /*+ PARALLEL(8) */ INDEX ix_ab ON t(a, b);
ALTER /*+ PARALLEL(8) */ TABLE t ADD INDEX ix_a(a), ADD INDEX ix_b(b);
```

`PARALLEL` / `PARALLEL()` / `PARALLEL(0)` use `MYDB_PARALLEL_DEGREE`;
`PARALLEL(1)` and `NO_PARALLEL` force serial execution. The precedence is
`MYDB_PARALLEL_DISABLE`, `NO_PARALLEL`, `PARALLEL(n)`,
`MYDB_PARALLEL_DEGREE`, and the built-in default of 4. The effective
degree of parallelism is further limited by the number of physical pages,
the number of indexes, file descriptors, and `StorageScratch` reservations
already acquired; multi-index builds share one worker budget and never
form an outer × inner thread pool.

Explicit `PARALLEL` currently supports only BTREE; FULLTEXT, SPATIAL,
HNSW, and IVF return `ER_NOT_SUPPORTED_YET`. Without a hint, those
indexes keep using their existing build strategies.

## 7. Query Runtime Hints

### 7.1 `MAX_EXECUTION_TIME(milliseconds)`

Sets a wall-clock execution limit for the current `SELECT`, overriding
the session variable `max_execution_time`:

```sql
SELECT /*+ MAX_EXECUTION_TIME(500) */ *
FROM large_table
WHERE expensive_predicate(payload);
```

- The unit is milliseconds, and the parameter type is unsigned integer.
- `MAX_EXECUTION_TIME(0)` explicitly disables the timeout for the current
  statement, even when the session variable is set to a nonzero value.
- When it occurs multiple times in one statement, the current
  implementation uses the first hint.
- The timeout takes effect through cooperative cancellation checks and,
  when triggered, returns MySQL error code 3024, SQLSTATE `HY000`.

This hint applies to `SELECT` only.

### 7.2 `SET_VAR(name=value)`

Currently only three statement-level temporary overrides are accepted,
all for `SELECT ... INTO OUTFILE/DUMPFILE`; they modify neither session
nor GLOBAL sysvars:

```sql
SELECT /*+ SET_VAR(select_into_buffer_size=1048576)
           SET_VAR(select_into_disk_sync=ON)
           SET_VAR(select_into_disk_sync_delay=0) */
       * INTO OUTFILE '/tmp/result.tsv'
FROM t;
```

| Name | Allowed Values |
| --- | --- |
| `select_into_buffer_size` | `8192..2147479552` bytes, rounded down to a multiple of 4096 bytes at execution time. |
| `select_into_disk_sync` | `1/0`, `ON/OFF`, or `TRUE/FALSE`. |
| `select_into_disk_sync_delay` | `0..31536000` milliseconds. |

Each `SET_VAR` must carry exactly one `name=value` parameter. The first
entry with a given name wins; an unknown variable produces a hint
warning, and an illegal value for a known variable returns
`ER_WRONG_VALUE_FOR_VAR` at execution time. For now, do not treat
`SET_VAR` as a general override mechanism for arbitrary system variables.

## 8. Vector Search Hints

These hints affect only queries recognizable as KNN, typically a single
ascending distance sort key with a constant `LIMIT`:

```sql
SELECT id
FROM items
ORDER BY DISTANCE(embedding, '[0.1,0.2,0.3]')
LIMIT 10;
```

### 8.1 `HNSW_EF(n)`

Overrides the base `ef_search` of the HNSW search for the current
statement:

```sql
SELECT /*+ HNSW_EF(200) */ id
FROM items
ORDER BY DISTANCE(embedding, '[0.1,0.2,0.3]')
LIMIT 10;
```

Valid values are positive integers. `0` means no override. The initial
effective search breadth is
`max(base ef, mydb_hnsw_ef_dynamic_factor × k, k)`.

The precedence of the base value is:

1. `HNSW_EF(n)`;
2. the session `mydb_hnsw_ef_search` when explicitly SET (even to 64), or
   configured away from the built-in default;
3. `ef_search` in the index WITH options;
4. the built-in default of 64.

An explicit `HNSW_EF(n)` also skips the "small table / low filtered row
count automatically switches to brute force" threshold, for cases that
explicitly ask for the HNSW path; hard preconditions such as a missing
index, a metric mismatch, or a pending PQ model still cause fallback. This
hint changes search breadth; it does not change quantization or the index's
`rerank_mult`. The re-ranking pool is also subject to session candidate and
memory budgets. See [vector query tuning](vector-indexes.md#query-tuning).

### 8.2 `NO_VECTOR_INDEX`

Prevents the current KNN query from using HNSW or IVF
`VectorIndexScan` and runs an exact brute-force distance scan instead
(falling back to a regular sequential scan plus sort when the dedicated
brute path is unavailable):

```sql
SELECT /*+ NO_VECTOR_INDEX */ id
FROM items
ORDER BY DISTANCE(embedding, '[0.1,0.2,0.3]')
LIMIT 10;
```

This hint takes no parameters. It suits small-table queries, recall
checking, and differential verification of ANN versus exact results.

### 8.3 `IVF_NPROBE(n)`

Overrides the number of inverted cells probed by the IVF query of the
current statement:

```sql
SELECT /*+ IVF_NPROBE(32) */ id
FROM items
ORDER BY DISTANCE(embedding, '[0.1,0.2,0.3]')
LIMIT 10;
```

Valid values are positive integers, clamped at execution time to
`1..=nlist`; `0` means no override. A larger nprobe usually means higher
recall and higher scan cost. The precedence is:

1. `IVF_NPROBE(n)`;
2. the session `mydb_ivf_nprobe` when explicitly SET (even to 8), or
   configured away from the built-in default;
3. `nprobe` in the index WITH options;
4. the built-in default of 8.

This hint affects IVF only; HNSW remains controlled by `HNSW_EF`.
Full probing of unquantized IVF scores all lists using native values.
Quantized IVF retains approximate candidate scores, so probing every list
with a finite re-ranking pool does not guarantee the global exact TopK.
Use `NO_VECTOR_INDEX` for an exact comparison.

## 9. Diagnostics and Verification

Invalid hints usually do not fail the `SELECT`; they are ignored and
surface as hint warnings. This includes:

- unknown hint names;
- missing parameters, illegal integers, or too many parameters;
- the table referenced by the hint is not in the current `FROM`;
- the index referenced by the hint does not exist;
- duplicate/conflicting entries within one conflict group, for example
  `PARALLEL` versus `NO_PARALLEL`, or `USE_INDEX` versus `FULL_SCAN` on
  the same table;
- `ZONE_MAP` versus `NO_ZONE_MAP` conflicts on the same table;
- `HASH_JOIN` versus `NO_HASH_JOIN` conflicts on the same table set;
- `PARALLEL` not applied because no physical operator qualifies or the
  effective degree of parallelism is 1.

Hint diagnostics are shown through `EXPLAIN`:

- traditional format: `hint warning: ...` is appended to the `Extra`
  column of the first row;
- `FORMAT=TREE`: one or more `hint warning: ...` lines are printed before
  the plan tree;
- `FORMAT=JSON`: the `query_block.hint_warnings` array.

Example:

```sql
EXPLAIN FORMAT=TREE
SELECT /*+ USE_INDEX(t missing_idx) BOGUS_HINT */ *
FROM t
WHERE a = 1;
```

For scan hints that were adopted, the access type, `key`, TREE node, or
the `FULL_SCAN`, `INDEX_FFS(...)`, `USE_INDEX(...)`, and table-level
`USE/FORCE/IGNORE INDEX` notes in `EXPLAIN` confirm the actual plan.

## 10. DML Compatibility Boundaries

MySQL allows optimizer comments after the keywords of `INSERT`, `UPDATE`,
and `DELETE`. mydb currently handles these statements with the compatible
behavior "parse and accept, but do not consume":

```sql
INSERT /*+ ... */ INTO t VALUES (1);
UPDATE /*+ ... */ t SET v = 1 WHERE id = 1;
DELETE /*+ ... */ FROM t WHERE id = 1;
```

Consequently, even hints listed on this page do not change the plan or
runtime behavior of these DML statements; `MAX_EXECUTION_TIME` likewise
installs a deadline only for `SELECT`. Parse warnings and the "hint
accepted but ignored for DML" diagnostics are written to the current
statement's warnings rather than being silently swallowed. To control
bulk-load parallelism, use the form that is explicitly effective:

```sql
LOAD DATA /*+ PARALLEL(n) | NO_PARALLEL */ ...
```

`STRAIGHT_JOIN` belongs to MySQL SELECT modifier / JOIN operator syntax,
not to `/*+ ... */` hints. `SELECT STRAIGHT_JOIN` fixes the FROM join
order of the current query block; `a STRAIGHT_JOIN b` only constrains
that join's left operand to precede its right operand, while other
commutative inner joins are still chosen by the optimizer. Point lookups
on a complete unique key can be resolved early following MySQL's `const`
table rules. Neither spelling guarantees the order of result rows; use
`ORDER BY` when a stable row order is required.

## 11. Complete Example

```sql
EXPLAIN FORMAT=TREE
SELECT /*+
          ORDERED
          USE_INDEX(o idx_customer)
          INDEX_NLJ(o, l)
          NO_INDEX(l idx_old)
          PARALLEL(4)
          MAX_EXECUTION_TIME(2000)
       */
       o.id,
       SUM(l.amount)
FROM orders AS o
JOIN order_lines AS l ON l.order_id = o.id
WHERE o.customer_id = 100
GROUP BY o.id
ORDER BY o.id;
```

When reading the plan, check each of the following:

1. whether `orders` uses `idx_customer`;
2. whether `order_lines` becomes the inner table of `IndexNestedLoopJoin`
   and does not use `idx_old`;
3. whether `Exchange` / parallel scan or aggregate nodes appear;
4. whether hint warnings appear at the top of the plan;
5. whether error 3024 is returned when actual execution exceeds 2
   seconds.

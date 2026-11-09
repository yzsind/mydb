# Optimizer and Query Execution

This guide explains access paths, joins, execution modes, and how to diagnose
query plans. Use [Hints](hints.md) for hint syntax and
[Configuration](configuration.md) for optimizer and execution settings.

## Planning and Statistics

mydb selects physical plans from the SQL, schema, available indexes, statistics,
session semantics, optimizer settings, and valid hints. Plans specify access
paths, join order and algorithms, and parallel topology. Runtime memory pressure
can change batches, prefetching, and spill behavior within an operator.

Statistics include row counts, distinct counts, NULL fractions, value
frequencies, and histograms. Missing statistics use conservative estimates.
After substantial DML, EXPLAIN can mark statistics as stale. Run
[`ANALYZE TABLE`](statements/admin/analyze-table.md) and compare estimated with
actual rows before changing hints or cost settings.

Estimates describe expected output, while access-row estimates describe the
rows scanned or probed. A scan can read a million rows and return only one
hundred. Join fanout, skew, stale statistics, and missing correlation information
can make estimates differ from observations.

Costs rank alternative plans; they are not elapsed milliseconds. JSON EXPLAIN
exposes cost, memory, and spill estimates for diagnosis. Inspect cost settings
with `SHOW VARIABLES LIKE 'cbo_%'`; global changes can invalidate cached plans
and should be evaluated against representative application queries.

The [RRF fusion operator](hybrid-search.md) plans each ordered branch
independently under one statement snapshot and memory budget. EXPLAIN keeps
branch access paths visible; ANALYZE reports candidate counts, overlap, timing,
and reserved memory. An outer retrieval window does not shorten branch windows
or bypass relational filters, joins, aggregation, or windows.

## Scan and Access Path Optimization

### Candidate Generation

Partition pruning removes irrelevant partitions before access paths are compared.
Ordinary B-tree candidates include:

- Unique or non-unique equality `IndexLookup`;
- `IndexMultiLookup` for IN-lists;
- `IndexRangeScan` on a single column or a composite prefix;
- `IndexFastScan` covering the query;
- Index scans with no predicate that still provide ORDER BY/GROUP BY order;
- `IndexInLookup` driven by a subquery result;
- Sequential heap scans and Zone Map page-range scans.

Candidates are compared not merely on "whether an index exists" but on how
many leaf/heap pages must be read, whether the index covers the query, output
width, index-to-heap correlation, whether an upper-level Sort/Hash Aggregate is
eliminated, LIMIT row goals, and hints. A low-selectivity index may lose to a
sequential scan; an ordered or covering index that eliminates expensive
upper-level operators may also win the complete plan even when its local scan
cost is higher.

### Scan Operators

| Operator | Data source and use | Key semantics |
| --- | --- | --- |
| `ValuesScan` | `VALUES` or constant relations with no FROM | Expressions are evaluated lazily per row; supports parameters, warnings, and cancellation |
| `JsonTableScan` | `JSON_TABLE` | Expands JSON after the operator opens; column definitions belong to the plan |
| `SystemScan` | information/performance/mysql/sys relations | Catalog rows participate in joins/aggregation/sorting through the ordinary physical pipeline |
| `CteScan` | Materialized CTE/derived results | Supports filter and column pruning; reuses the CTE registry |
| `GenerateSeries` | Integer, fixed-point numeric, and timestamp series | Can carry ordinality and a residual filter |
| `SeqScan` | Sequential heap scan | Baseline for MVCC, predicates, runtime filters, partition and column pruning |
| `RowIdLookup` / `RowIdBatchLookup` | Direct SQL ROWID heap access | Decodes the table-local partition tag and page/slot, preserves MVCC and residual predicates, and requires no B-tree probe; see [SQL ROWID](rowid.md) |
| `ZoneMapScan` | Zone Map candidate pages + heap scan | The Zone Map prunes pages only; the original predicate must be rechecked row by row; falls back safely when metadata is unavailable |
| `IndexLookup` | Single-key point lookup | `exact` distinguishes exact semantics; can be covering or fetch back to the heap |
| `IndexMultiLookup` | Multi-key/IN point lookups | Deduplication, ordering, and residual semantics are kept correct by the executor |
| `IndexMerge` | RowId union/sort-union/intersection across multiple B-trees of the same table | After merged deduplication, performs unified MRR heap fetches and rechecks the full predicate; supports partitioning, DML, and spill |
| `IndexRangeScan` | Bounded/unbounded B-tree ranges | Can run forward or backward; reads leaves directly when covering, otherwise windowed MRR heap fetches |
| `IndexFastScan` | Full or range scans over covering indexes | Reads only B-tree leaves and never touches heap payloads |
| `IndexInLookup` | Index driven by `column IN (subquery)` | The subquery first forms a key set, then prefix+IN probes run |
| `FulltextScan` | FULLTEXT postings and BM25 | MATCH postings produce candidates while the ordinary residual still rechecks; can be combined with B-tree/Zone Map candidate intersection |
| `SpatialScan` | MBR candidates from persistent R-tree pages | The R-tree returns a superset; exact spatial predicates must be rechecked |
| `VectorIndexScan` | HNSW/IVF vector KNN | After candidates are generated, re-ranks using MVCC-visible heap vectors; the approximate index affects recall but never relaxes visibility |
| `VectorBruteScan` | Exact brute-force KNN | Scans all visible vectors with a bounded Top-K; materializes only the winners |

Covering access still respects heap MVCC visibility. A partition's local
index order is not automatically a global order; cross-partition results can
require a Sort.

### Safety Boundaries of Special Access Paths

- Zone Maps, spatial indexes, full-text pre-filtering, and runtime Bloom
  filters are candidate filters, not final semantic decisions.
- HNSW/IVF are approximate candidate generators; the executor reads
  currently visible native heap values and recomputes distances. Explicit
  SQ8/SQ4/PQ/BQ payloads affect candidate scores, while `quantization=none`
  retains native scores. Search breadth and candidate retention can still
  limit recall after exact re-ranking. Unusable coverage and pending PQ
  training fall back to exact scanning; JOIN, aggregation, DISTINCT, and
  window queries retain their complete relational input. See
  [vector query tuning and diagnostics](vector-indexes.md).
- Temporal `FOR SYSTEM_TIME AS OF` uses a historical snapshot; when historical
  indexes are degraded it falls back to heap/undo, and it must never return
  the current version in place of a historical one.
- A covering index requires that every column still needed by the projection,
  the predicate, and upper levels be provided by valid key/INCLUDE columns; a
  prefix index must not impersonate coverage of full values.
- Hints such as `FULL_SCAN`, `INDEX_FFS`, and `USE_INDEX` influence candidates
  only when semantically feasible; see [`hints.md`](hints.md) for the detailed
  conflict and warning rules.

## Join Optimization

The optimizer compares legal join orders and algorithms using cardinality,
available indexes, ordering, memory, and hints. Outer joins, SEMI/ANTI joins,
and correlated Apply operators constrain reordering to preserve SQL semantics.

### Join Algorithm Selection

| Operator | Applicable shape | Execution characteristics |
| --- | --- | --- |
| `NestedLoopJoin` | Arbitrary conditions, small inputs, no usable equality hash/index path | Materializes or caches the inner side and evaluates the condition pair by pair; cost grows with the product of both sides |
| `IndexNestedLoopJoin` | The right side's join key matches an index prefix | Outer keys drive point/range probes; supports covering access, column pruning, and batched probes |
| `HashJoin` | One or more equality/NULL-safe keys can be extracted | Chooses a build side, hashes the keys, then probes; the non-equality part remains a residual |
| `AdaptiveJoin` | Estimates are uncertain and multiple algorithms are legal | The plan explicitly retains Hash/INLJ/NLJ candidates and a preference, and switches safely inside the node based on measured build/outer sizes |

The Hash Join build side is chosen by weighing the join type, row counts and
widths of both sides, input costs, output preservation rules, and spill risk.
SEMI/ANTI without a residual can use a key-only set that carries no right-side
payload. At runtime, a Bloom filter, an exact integer set, or a key set can be
injected into the probe scan; the parent join always remains the final
correctness check.

When memory is insufficient, Hash Join uses Grace partition spill. A hot
partition that remains too large can be recursively repartitioned, protected by
bounded depth, grants, and quotas; when it cannot continue safely, an explicit
error is returned. AdaptiveJoin's algorithm switch is part of that physical
node's definition; the executor is not allowed to re-plan arbitrary ordinary
join nodes on its own.

## Aggregation, DISTINCT, Sorting, and Windows

### Aggregation Selection

An ordinary GROUP BY compares Hash Aggregate against Sort + Streaming
Aggregate; if the input is already ordered by the group key, Streaming can be
chosen directly. Per-group state size is estimated from the actual aggregate
functions; `COUNT(*)`, `GROUP_CONCAT`, and DISTINCT aggregates cannot share a
single fixed width.

| Operator | Applicable condition | Memory and ordering |
| --- | --- | --- |
| `HashAggregate` | General GROUP BY/unordered input | Builds state keyed by group key; supports partition spill |
| `StreamingAggregate` | Input is contiguous by group key | Keeps only the current group, approximately O(state of a single group) |
| `HashRollupAggregate` / `StreamingRollupAggregate` | `WITH ROLLUP` | Use hash or prefix-ordered accumulation, respectively |
| `HashGroupingSetsAggregate` / `StreamingGroupingSetsAggregate` | Grouping sets | Prefix chains can stream; otherwise the hash path is used |
| `IndexAggregate` | Narrow single-table shapes of COUNT/MIN/MAX/COUNT DISTINCT | Consumes index boundaries or leaves directly without producing ordinary input rows |
| `CoveringIndexAggregate` | All columns needed by the group/aggregate are covered by the index | Scans leaves and updates state directly; can be bounded to a range |
| `ParallelHashAggregate` | Large, parallelizable hash aggregation | Partitions by group hash, guaranteeing each group belongs to exactly one worker |
| `ParallelIndexAggregate` | Splittable covering-index aggregation | Workers process leaf slices; the coordinator merges partial state |

Row-based and vectorized aggregation preserve the same overflow, scale, NULL,
and grouping semantics. Streaming aggregation requires a compatible key order.

DISTINCT uses `StreamingDistinct` when the input is already sorted by the
projection keys; otherwise `Projection { distinct: true }` uses spillable
hash-deduplication semantics.

### Sorting and LIMIT

| Operator | Selection condition | Complexity/characteristics |
| --- | --- | --- |
| `Sort` | A complete global sort is required | Precomputes fallible sort keys; in-memory runs or external merge |
| `IncrementalSort` | The input already satisfies a non-empty ORDER BY prefix | Sorts only the remaining suffix within each equal-prefix group |
| `TopK` | `ORDER BY ... LIMIT` with a small enough `offset + limit` | Bounded heap, O(k) memory, followed by ordered output |
| `TopWindow` | Better than maintaining a huge TopK for deep pagination | Quickselect/windowed selection; outputs only the requested page |
| `Limit` | Ordinary LIMIT/OFFSET | Stops early on streaming input; a parameterized limit is rebound at execution time |
| `ParallelSort` | Large sorts that can run in parallel | Workers sort locally; the coordinator performs a k-way merge |
| `ParallelTopK` | Small k over parallel input | O(k) per worker; at most `degree × k` rows are merged at the end |

If a B-tree, a KNN scan, or an existing Sort already provides the full
required order, the planner eliminates the redundant Sort. When only a partial
prefix is provided, IncrementalSort can be used. Sort expressions are
evaluated once per row and the keys are saved; comparators must not re-execute
expressions that could error, produce warnings, or call stored functions.

### Projection, Filter, and Window

| Operator | Purpose |
| --- | --- |
| `Projection` | Computes SELECT columns, aliases, and optional hash DISTINCT |
| `Filter` | Applies genuine post-scan/post-join WHERE/HAVING filtering to the input result |
| `Apply` | Executes correlated subquery predicates that could not be decorrelated, with batched reuse by correlation key |
| `WindowAgg` | Groups window specifications by shared `(PARTITION BY, ORDER BY)`, reusing a single sort |

`ROW_NUMBER`, `RANK`, and `DENSE_RANK` can carry per-partition Top-N
information when a provable upper-bound predicate holds, retaining only the
necessary prefix. Window frame, peer, NULL, and ordering semantics are still
validated by the full window implementation.

## Set Operations, CTEs, and Materialization Boundaries

| Operator | Behavior |
| --- | --- |
| `SetOp` | `UNION ALL` can concatenate streams; UNION DISTINCT, INTERSECT, and EXCEPT use spillable hash-count/deduplication state |
| `Materialize` | An explicit, reusable, spillable pipeline boundary for multiple scans of a CTE/derived relation |
| `RecursiveUnion` | The anchor runs once; the recursive part iterates over the previous round's delta until the delta is empty or the maximum depth is reached |
| `CteScan` | Reads materialized results from the active registry; supports filter and column pruning |

The strategy for non-recursive CTEs weighs reference count, body cost, and
volatility: a single reference is usually inlined; recursive CTEs and CTEs
containing volatile functions must be materialized; with multiple references,
the cost of one materialization plus multiple scans is compared against
inlining each reference. A recursive `UNION DISTINCT` CTE must additionally
deduplicate against historical results.

EXPLAIN represents materialization explicitly, with its associated costs,
memory use, and spill behavior.

## Parallel Plans and Exchange

Parallel nodes are never exposed bare in the tree; parallel scan nodes are
usually collected by an `Exchange`:

| Operator | Parallel partitioning |
| --- | --- |
| `ParallelSeqScan` | Disjoint heap page ranges/morsels |
| `ParallelZoneMapScan` | The coordinator generates candidate ranges; workers dynamically claim morsels |
| `ParallelIndexFastScan` | Disjoint B-tree leaf slices; output is unordered |
| `ParallelIndexRangeScan` | Leaf slices + in-worker heap fetches; output is unordered |
| `ParallelIndexAggregate` | Partial aggregates over leaf slices |
| `ParallelHashAggregate` | Hash partitioning by group key |
| `ParallelRollupAggregate` / `ParallelGroupingSetsAggregate` | Executed per partition, then merged per the corresponding aggregation semantics |
| `ParallelSort` / `ParallelTopK` | Local sort or local Top-K over input chunks |
| `Exchange` | Unordered concatenation, or a sorted k-way merge over locally ordered input |

Automatic parallelism considers input size, page count, and the expected
benefit after worker setup costs.
`PARALLEL(n)` can request a degree and `NO_PARALLEL` forbids the rewrite;
order-sensitive ancestors such as natural index order, streaming aggregation,
and ordered LIMIT block parallel scans that would destroy the order. At
execution time, only morsels and batches can be adjusted within the
established topology; an Exchange must not be added or removed because of the
current load.

## Reusable Plans and the Plan Cache

A cached plan retains the normalized query, physical operators, object
dependencies, and reusable expressions. The cache key
includes the typed/auto-parameterized SQL fingerprint, the current database,
and the session semantics that affect semantics and plans. The following
changes invalidate a cache entry or prevent a hit:

- DDL, catalog/schema version, or dependent-object changes;
- CBO epoch, statistics, or related engine epoch changes;
- Semantic changes in key components such as `sql_mode`, time zone,
  character set/collation, and `optimizer_switch`;
- An incompatible plan signature of prepared parameters or LIMIT magnitude
  bucket.

Parameters are still bound lazily at execution time. Parameters inside index
keys, LIMIT/OFFSET, and vector query expressions are re-resolved in the
current execution context; values from the first execution must never be
permanently baked into result semantics.

## Execution Modes and Resource Limits

### From Plan to Execution

| Execution mode | Typical use | Fallback behavior |
| --- | --- | --- |
| Compiled fast path | Single-table unique point lookups, index range/multi lookups, fused scan/filter/projection/LIMIT, and supported range aggregates | Other query shapes use general physical execution |
| Row-based execution | Supported queries that require scalar processing | Unsupported SQL returns an explicit error |
| Batch/vector execution | Contiguous groups of supported operators and expressions | Scalar evaluation is used where vector processing is unavailable |
| Parallel execution | Eligible scans, joins, aggregation, and sorting | Degree of parallelism and ordering requirements constrain worker use |

All execution modes preserve SQL values, NULL handling, errors, exact decimal
arithmetic, time-zone rules, and collation semantics. Selecting another mode
does not enable otherwise unsupported SQL.

### Errors and Resource Limits

Unsupported SQL returns an explicit error. Evaluation failures, I/O errors,
corruption, and quota failures are reported rather than converted into NULLs
or empty results.

Query memory is governed by shared and per-query budgets. Spillable operators
can move intermediate data to temporary files at a memory soft watermark.
When required memory cannot be admitted, the query returns MySQL error
1037/HY001. Temporary-disk quotas can also cause an explicit error.

See [Server Limits](limits.md) and [Configuration](configuration.md) for
budgets and configurable ranges.

### Statement Context and Streams

All workers of a statement share its parameters, session settings, snapshot,
partition pruning, cancellation, deadline, and resource budgets. Streaming
operators deliver bounded batches so that results can be consumed incrementally.
Parallel lookups preserve required ordering even when reads complete out of order,
and partition pruning is consistent across all workers.

A query that finishes, fails, reaches a limit, or is cancelled releases its
workers, temporary files, and memory reservations. Cancellation and timeouts
are checked during execution; they do not leave detached query work running.

### Vectorization Boundaries

`MYDB_VECTORIZED=off|on|auto` controls scalar versus vector processing without
changing the physical plan topology:

- `off` uses scalar expression and aggregate processing within the batch
  pipeline. It does not force every query through a row-based executor.
- `on` and `auto` use typed vector processing where supported and scalar
  processing for other expressions.
- Queries can cross row/batch boundaries as required by their operators.

Results and SQL semantics remain consistent across these modes. To inspect
the chosen operators and runtime behavior, use `EXPLAIN` or `EXPLAIN ANALYZE`.

### Columns and Runtime Statistics

Qualified and unqualified column names follow the same resolution rules
across execution modes. Ambiguous references return errors. Aliases retain
their SQL scope and do not change meaning when execution is parallelized.

`EXPLAIN ANALYZE` attributes runtime statistics to the corresponding physical
plan nodes. Compare estimated and actual rows to identify expensive scans,
join fanout, and unexpected materialization.

### Blocking Operators and Spill

Sort, Hash Join build, Hash Aggregate, DISTINCT, window partitioning, and
materialized set operations or CTEs may retain intermediate state before
returning results. Supported operators spill to disk when memory is limited;
blocking execution does not require the entire input to stay resident.

Sort evaluates ORDER BY expressions before comparing keys. Hash Join may
partition its build input for disk replay. If an oversized partition cannot
be reduced further, configuration determines whether execution retries with
a bounded build or returns an error. Corrupted spill data and disk quota
failures return explicit errors.

Temporary files and retained state are released on success, failure,
cancellation, or early consumer exit. Consult [Configuration](configuration.md)
for spill limits and the plan diagnostics below.

## How to Read and Diagnose Plans

### EXPLAIN Formats

```sql
EXPLAIN SELECT * FROM orders WHERE customer_id = 42;

EXPLAIN FORMAT=TREE
SELECT customer_id, COUNT(*)
FROM orders
WHERE created_at >= '2026-01-01'
GROUP BY customer_id
ORDER BY COUNT(*) DESC
LIMIT 20;

EXPLAIN FORMAT=JSON
SELECT * FROM a JOIN b ON a.id = b.a_id;

EXPLAIN ANALYZE
SELECT * FROM orders WHERE customer_id = 42;
```

- TRADITIONAL is suited to quickly confirming access type, possible keys,
  chosen key, rows, filtered, and Extra.
- TREE is suited to confirming operator parent-child relationships, join
  build/probe sides, and sort/aggregation/parallelism boundaries.
- JSON is suited to inspecting structured costs, estimate sources, memory/
  spill estimates, and machine processing.
- ANALYZE actually executes the SQL and adds observations such as actual rows,
  loops, first-row/completion times, query wall time, operator-self I/O,
  spill, and runtime filter rejections; used on DML, it really modifies data.
  The instrumentation follows the same cache, compilation, vectorization, and
  parallel routing as ordinary statements and does not participate in plan
  selection.

When reading a plan, answer four questions from leaf to root:

1. Does each scan's `access_rows` match the filtering selectivity, and does
   the scan use the expected partition/index?
2. Does the join order reduce intermediate results first, and do the
   algorithm and build side match the cardinality, indexes, and widths?
3. Do ORDER BY/GROUP BY/DISTINCT reuse an existing order, or do they produce
   the necessary blocking operators?
4. At which lowest node do estimates and actuals start to diverge noticeably?
   Fixes should usually start from that node's statistics, predicate
   estimation, or constraint modeling rather than force-hinting the topmost
   operator.

### Optimizer Trace

When you need to understand "why a candidate lost", enable the bounded
optimizer trace:

```sql
SET optimizer_trace = 'enabled=on';

SELECT customer_id, COUNT(*)
FROM orders
GROUP BY customer_id;

SELECT QUERY, TRACE, MISSING_BYTES_BEYOND_MAX_MEM_SIZE
FROM information_schema.optimizer_trace;
```

The trace records at most 256 events and 1 MiB of JSON, covering candidate
costs, join enumeration, aggregation selection, final plan properties, and
truncation information. Querying the trace table itself does not overwrite the
trace of the previous statement under examination.

### Common Troubleshooting Order

1. Confirm SQL semantics, parameter values, hint warnings, and session
   variables.
2. Run or check `ANALYZE` on the involved tables and compare estimated/actual
   rows.
3. Use JSON to inspect each scan's `rows`, `access_rows`, width, I/O, and
   spill estimates.
4. Use the trace to determine whether a candidate was never generated,
   rejected as semantically illegal, or lost the cost comparison.
5. Diff results between `MYDB_VECTORIZED=off` and the default mode to separate
   planning problems from kernel problems.
6. If necessary, compare alternative plans with hints and record the query,
   statistics, session settings, and EXPLAIN output when reporting a problem.

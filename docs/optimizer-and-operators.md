# Optimizer and Execution Operators

This document explains how mydb selects and executes query plans, how to read
`EXPLAIN` output, and how statistics, hints, and resource limits affect a query.
For execution modes, cancellation, and spill behavior, see
[SELECT Execution](executor.md); for hint syntax, see [Hints](hints.md).

## 1. Overall Architecture

A SELECT passes through roughly the following stages from SQL to results:

```text
SQL / prepared statement
        │
        ▼
Parsing, name resolution, semantic checks, session semantics pinning
        │
        ▼
AST normalization and decorrelation
  ├─ USING/NATURAL JOIN expansion
  ├─ GROUP BY / ROLLUP binding
  ├─ predicate normalization, constant simplification, OR→IN
  ├─ ENUM/SET numeric semantics rewrite
  └─ subquery → SEMI/ANTI JOIN; anything not safely rewritable stays as Apply
        │
        ▼
Logical plan (describes the relational algebra shape of the query)
        │
        ▼
CBO physical planning
  ├─ predicate pushdown, partition pruning, column pruning
  ├─ access path and index selection
  ├─ join order, join algorithm, and build side
  ├─ aggregation, sort, Top-N, window, DISTINCT
  └─ parallel rewrite, cost synchronization, stable plan node identifiers
        │
        ▼
Reusable physical plan / plan cache
        │
        ▼
Unified physical plan execution
  ├─ compiled SELECT kernel (supported narrow shapes)
  ├─ batch/vector kernels
  ├─ row-based execution and row/batch conversion
  ├─ streaming and parallel pipelines
  └─ in-memory execution or spill for blocking operators
        │
        ▼
Results, warnings, EXPLAIN ANALYZE runtime statistics
```

The logical plan describes scans, filters, joins, aggregation, projection,
sorting, and limits. The optimizer compares valid physical alternatives using
the normalized query, catalog, statistics, and hints. The chosen plan specifies
access paths, join order, algorithms, and parallel execution.

## 2. Optimizer Inputs, Outputs, and Stability

### 2.1 Valid Planning Inputs

A physical plan may be determined by the following information:

- The normalized SQL shape, constants, and the plan signature of parameters;
- Schema, indexes, partition definitions, and the catalog version;
- Table, column, histogram, and index statistics produced by `ANALYZE`;
- Session state that affects semantics, such as `sql_mode`, time zone,
  character set, collation, and `optimizer_switch`;
- Optimizer hints and `cbo_*` cost coefficients;
- The fixed, versioned planning resource baseline.

The same SQL, schema, statistics, CBO configuration, hints, and session
semantics must produce the same physical plan. Live RSS, current concurrency,
buffer pool residency, actual work-area grants, and runtime memory pressure
must not change scan type, join order, join algorithm, or parallel topology.

### 2.2 Fixed Planning Resource Baseline

The CBO evaluates likely cache hits and spills against a fixed baseline rather
than reading runtime resources:

| Item | Current planning baseline | Purpose |
| --- | ---: | --- |
| page size | 16 KiB | Unit for I/O and spill costs |
| effective cache | 65,536 pages | Stable cache model for index/heap access |
| query memory | 256 MiB | Planning-time spill decision for Hash Join, Hash Aggregate, and Sort |
| non-covering range MRR window | 131,072 RIDs | Cost estimation for non-covering range scans |
| batched INLJ chunk | 16,384 outer rows | Cost estimation for batched index probes |

The executor may shrink batches, MRR windows, or prefetching inside a node, or
trigger spill, but it must not replace a physical node that has already been
decided. The planning baseline may change between releases.

### 2.3 Physical Plan Output

Each physical plan node describes:

- Operator arguments and child nodes;
- `estimated_rows`, the number of rows the node is expected to output;
- The accumulated cost;
- For scan nodes, also `access_rows`, the number of rows expected to be
  actually accessed, decoded, or probed;
- Physical properties such as ordering, degree of parallelism,
  parameterization, and replayability;
- A stable plan node identifier in the final plan, used to correlate EXPLAIN ANALYZE
  statistics and execution details.

`estimated_rows` and `access_rows` must not be conflated. For example, a
sequential scan with a low-selectivity predicate may output only 100 rows while
still accessing one million rows; parallel thresholds and scan CPU should be
based on the latter.

## 3. Semantic Preparation and Relational Rewrites

Before access path selection, the planning entry point performs a set of
correctness-first transformations:

1. Expand `USING` / `NATURAL JOIN` and establish merged-column semantics.
2. Resolve GROUP BY aliases/ordinals and bind ROLLUP and grouping sets.
3. Validate column references, ambiguity, scopes, and window definitions.
4. Uniformly rewrite numeric comparisons and function arguments for ENUM/SET.
5. Normalize boolean predicates, including NOT pushdown, constant
   short-circuiting, invalid BETWEEN simplification, and rewriting OR equality
   chains into IN.
6. Propagate small-table constant filters along safe inner-join equality
   chains so that fact tables get a more accurate effective cardinality before
   join enumeration.
7. Decorrelate provably equivalent `IN`, `EXISTS`, `NOT EXISTS`, and
   correlated subqueries into SEMI/ANTI JOINs or derived relations.
8. Predicates that still carry correlated dependencies are demoted to `Apply`,
   which the executor evaluates with batched parameterization and
   correlation-key memoization; they must never be silently ignored.

Predicate pushdown respects the null-extension semantics of outer joins.
Conditions that reference only the preserved side, ON conditions that reference
only the null-supplied side, and post-join WHERE conditions have different
safety boundaries. A WHERE condition that cannot be pushed down must remain a
true `Filter`; it must not be folded into the outer join residual, which would
incorrectly turn "filtered out" into "kept after NULL extension".

## 4. Statistics and Cardinality Estimation

### 4.1 Statistics Source Priority

Selectivity estimation uses a layered fallback:

| Layer | Source of information | Typical use |
| --- | --- | --- |
| L1 | Equi-depth histograms, TOP-K/MCV frequencies | Ranges, hot equality lookups, skewed distributions |
| L2 | NDV, NULL fraction, min/max, index-prefix NDV | Ordinary equality and range predicates, GROUP BY, joins |
| L3 | `cbo_default_*_selectivity` | Stable defaults when no usable statistics are available |

`RelEstimate` also records the `EstimateSource` and a confidence value in
`[0,1]`, used for diagnostics and conservative fallback; confidence is not
applied as an opaque cost multiplier.

### 4.2 Common Estimation Rules

- `col = constant` reads TOP-K/value frequencies first, converting the
  constant according to the column type and honoring the column collation.
  Full counts and sampled counts are estimated from frequencies, without using
  the column-wide average frequency as a lower bound; approximate candidate
  counts are conservatively corrected within the recorded count-error bounds.
  On a miss, the remaining row count and remaining NDV are used; a value
  absent from a complete exhaustive frequency table is estimated as zero,
  while a value absent from a complete sampled frequency table keeps an
  estimation allowance corresponding to one sample row.
- Low-cardinality columns retain complete sampled frequencies for at most
  `MYDB_ANALYZE_TOPK` physical values, unaffected by the row-count cap of the
  small-table exact mode and by TOP-K secondary row sampling; once capacity is
  exceeded, the frequency table is released and bounded approximate counting
  is used. Parallel merging and statistics persistence retain the frequency
  source, sample row count, completeness, and count error.
- Single-column constant index lookups (including single-column full prefixes
  of composite indexes) compute read cost from the corresponding value
  frequencies; multi-point lookups sum the cost across keys. Unique keys,
  composite keys, truncated string prefixes, and expression indexes keep the
  applicable index-cardinality estimation instead of misapplying value
  frequencies to a different key domain.
- Parameterized column predicates that have value-frequency statistics share
  compiled plans grouped into power-of-two buckets of the estimated row
  count, preventing an index nested loop planned for a low-frequency value
  from being reused for a high-frequency value; unique single-column equality
  point lookups keep cross-value reuse, and variants remain subject to the
  global LRU capacity limit.
- The `max-min+1` NDV upper bound and the dense-key-domain correction apply
  only to integer columns. Strings, decimals, and temporal types do not use
  the integer span; the BIGINT span is computed with integer subtraction
  first to avoid f64 precision loss.
- Range predicates prefer the histogram cumulative distribution, otherwise
  min/max linear interpolation.
- A constant-prefix LIKE can be converted into a range estimate; other LIKE
  predicates use the default selectivity and are charged higher expression
  CPU.
- AND/OR/NOT/IN/BETWEEN are estimated with composition rules and the
  independence assumption when multi-column correlation statistics are
  missing.
- Equality joins are estimated from both sides' NDV, uniqueness, domain
  overlap, and known constraints; SEMI/ANTI estimate "whether the outer key
  exists on the inner side", not the number of matches per key.
- The output estimate of a LEFT/RIGHT/FULL join must not be smaller than the
  cardinality of the preserved side; this is also the correctness lower bound
  for downstream memory estimation.
- GROUP BY prefers the index-prefix distinct count or column NDV, falling back
  to a stable default when they are missing.
- HAVING keeps both concepts of "how much aggregate state must be built" and
  "how many groups are expected to be output", preventing a selective HAVING
  from incorrectly shrinking Hash Aggregate memory.

After heavy DML on a table, EXPLAIN marks the statistics as stale. Stale
statistics can still participate in planning, but when troubleshooting a bad
plan, first run an appropriate `ANALYZE TABLE` and then compare estimated rows
with actual rows.

## 5. Cost Model

### 5.1 The Cost Vector

Every candidate uses a structured cost:

| Field | Meaning |
| --- | --- |
| `startup` | Work that must complete before the first row can be produced; for fully blocking operators, usually close to `total` |
| `total` | Accumulated cost of producing the full result; the primary basis for candidate comparison |
| `io_seq_pages` | Sequential page work diagnostics |
| `io_rnd_pages` | Random page work diagnostics |
| `cpu_units` | CPU work diagnostics for tuples, expressions, hashing, comparisons, etc. |
| `mem_bytes` | Estimated peak held memory |
| `spill_bytes` | Estimated size of state or input expected to be written to disk |

Cost units are anchored at `cbo_seq_page_cost = 1.0`. Ordinary candidates are
ordered by `total`; LIMIT, row-goal, and Top-N style paths additionally exploit
startup and the cost of obtaining the first k rows. Candidates with infinite,
NaN, or negative costs, or with `startup > total`, are invalid and cannot win.

### 5.2 Operator Cost Components

- Sequential scan: heap pages + decode/tuple CPU for accessed rows + predicate
  evaluation.
- Index point lookup/range: B-tree height, leaf pages, random heap reads,
  correlation, covering property, and MRR.
- Hash Join: build/probe inputs, key hashing, output, Bloom benefits, build
  state, and Grace spill.
- INLJ: outer rows × per-probe index cost, with separate modeling for batched
  probes, cached working sets, unique point lookups, and fanout.
- Sort: `N log N` comparisons/key encoding, input width, in-memory runs, and
  external merge passes.
- Hash Aggregate: input rows, group count, per-group accumulator/distinct
  state, and partition spill.
- Streaming Aggregate/Distinct: requires the input to already satisfy the
  ordering, with significantly smaller state.
- Parallel operators: parallelizable work divided by the degree, plus worker
  setup, aggregation, or merge costs; accumulated I/O diagnostics still
  represent total work.

The `cbo_*` coefficients can be inspected with `SHOW VARIABLES LIKE 'cbo_%'`.
Modifying global CBO coefficients advances the `cbo_epoch` and invalidates
previously cached plans. Apart from diagnostics and hardware calibration,
global coefficients should not be tuned for an individual benchmark.

## 6. Scan and Access Path Optimization

### 6.1 Candidate Generation

Base-table planning first performs static/procedural partition pruning and then
generates feasible candidates. Ordinary B-tree candidates include:

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

### 6.2 Scan Operators

| Operator | Data source and use | Key semantics |
| --- | --- | --- |
| `ValuesScan` | `VALUES` or constant relations with no FROM | Expressions are evaluated lazily per row; supports parameters, warnings, and cancellation |
| `JsonTableScan` | `JSON_TABLE` | Expands JSON after the operator opens; column definitions belong to the plan |
| `SystemScan` | information/performance/mysql/sys relations | Catalog rows participate in joins/aggregation/sorting through the ordinary physical pipeline |
| `CteScan` | Materialized CTE/derived results | Supports filter and column pruning; reuses the CTE registry |
| `GenerateSeries` | Integer, fixed-point numeric, and timestamp series | Can carry ordinality and a residual filter |
| `SeqScan` | Sequential heap scan | Baseline for MVCC, predicates, runtime filters, partition and column pruning |
| `ZoneMapScan` | Zone Map candidate pages + heap scan | The Zone Map prunes pages only; the original predicate must be rechecked row by row; falls back safely when metadata is unavailable |
| `IndexLookup` | Single-key point lookup | `exact` distinguishes exact semantics; can be covering or fetch back to the heap |
| `IndexMultiLookup` | Multi-key/IN point lookups | Deduplication, ordering, and residual semantics are kept correct by the executor |
| `IndexMerge` | RowId union/sort-union/intersection across multiple B-trees of the same table | After merged deduplication, performs unified MRR heap fetches and rechecks the full predicate; supports partitioning, DML, and spill |
| `IndexRangeScan` | Bounded/unbounded B-tree ranges | Can run forward or backward; reads leaves directly when covering, otherwise windowed MRR heap fetches |
| `IndexFastScan` | Full or range scans over covering indexes | Reads only B-tree leaves and never touches heap payloads |
| `IndexInLookup` | Index driven by `column IN (subquery)` | The subquery first forms a key set, then prefix+IN probes run |
| `FulltextScan` | FULLTEXT postings and BM25 | MATCH postings produce candidates while the ordinary residual still rechecks; can be combined with B-tree/Zone Map candidate intersection |
| `SpatialScan` | MBR candidates from the in-memory R-tree | The R-tree returns a superset; exact spatial predicates must be rechecked |
| `VectorIndexScan` | HNSW/IVF vector KNN | After candidates are generated, re-ranks using MVCC-visible heap vectors; the approximate index affects recall but never relaxes visibility |
| `VectorBruteScan` | Exact brute-force KNN | Scans all visible vectors with a bounded Top-K; materializes only the winners |

Index-only visibility treats the heap MVCC header as the source of truth; the
B-tree leaf's `page_max_trx_id` short-circuits only under safe conditions, and
no visibility map exists. A partitioned table's per-partition local index
order cannot automatically be treated as global order; cross-partition results
usually still require a Sort.

### 6.3 Safety Boundaries of Special Access Paths

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

## 7. Join Optimization

### 7.1 Join Graph and Join Order Search

For pure INNER/CROSS join islands, the optimizer treats base access paths as
the vertices of the join graph and cross-table predicates as edges. The search
has two levels:

1. First, a complete, connectivity-first multi-start greedy incumbent is
   built from multiple stably ordered relation seeds and predicate edge seeds.
2. When the number of relations is at most 12, DPsub enumerates connected
   subsets and their non-trivial bipartitions, allowing bushy trees; at most 8
   Pareto candidates with distinct physical properties are retained per
   subset.

The planning budget uses a deterministic operation allowance rather than a
wall-clock deadline affected by system scheduling. When the DP budget is
exhausted or the number of relations exceeds 12, the already-complete
multi-start incumbent is returned. Islands with 32 or more relations exceed the
current `u32` enumeration mask and use a separate connectivity-aware greedy
builder; this is an optimizer routing threshold, not a SQL table-count limit.

OUTER, SEMI, and ANTI joins are constrained by the preserved side, NULL
extension, and output semantics, and cannot be reordered freely; `ORDERED`, or
the need to preserve KNN driver order, also restricts join reordering.

### 7.2 Join Algorithm Selection

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

### 7.3 Join Result Width

Before join enumeration, the planner prunes columns from base scans so that
unused columns of wide tables do not enter build/probe costs; afterwards it
performs join-level output projection to keep multi-level joins from
repeatedly cloning columns that no longer have upper-level consumers. Column
pruning removes only unobservable data transfers; it must not remove columns
needed by filters, join keys, sorts, aggregation, windows, projections, or
warning-producing expressions.

## 8. Aggregation, DISTINCT, Sorting, and Windows

### 8.1 Aggregation Selection

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

Numeric finalization such as SUM/AVG reuses the common aggregate
implementation; row-based and vectorized paths must not define their own
overflow, scale, or NULL behavior. Grouping keys that cannot be safely
expressed by lexical order, such as the empty ENUM member, block incorrect
streaming plans.

DISTINCT uses `StreamingDistinct` when the input is already sorted by the
projection keys; otherwise `Projection { distinct: true }` uses spillable
hash-deduplication semantics.

### 8.2 Sorting and LIMIT

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

### 8.3 Projection, Filter, and Window

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

## 9. Set Operations, CTEs, and Materialization Boundaries

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

Materialization is not a side effect hidden inside a dispatcher but an
explicit node in the physical plan, so EXPLAIN, cost, memory reservation,
spill, and cancellation all attach to the same node.

## 10. Parallel Plans and Exchange

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

Automatic parallelism requires a stable size threshold, a minimum page count,
and a benefit comparison that includes setup cost to hold simultaneously, with
roughly 10% hysteresis retained to avoid thrashing near-threshold plans.
`PARALLEL(n)` can request a degree and `NO_PARALLEL` forbids the rewrite;
order-sensitive ancestors such as natural index order, streaming aggregation,
and ordered LIMIT block parallel scans that would destroy the order. At
execution time, only morsels and batches can be adjusted within the
established topology; an Exchange must not be added or removed because of the
current load.

## 11. Execution Model

### 11.1 Reusable Plans and the Plan Cache

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

### 11.2 The Three Execution Kernels

The same physical plan can use different execution modes, but the
result semantics must be identical:

| Kernel | Positioning | When not applicable |
| --- | --- | --- |
| Compiled SELECT fast path | Proven shapes such as single-table unique point lookups, range/multi lookups, fused scan-filter-project-limit, and narrow aggregation | Reports inapplicability before reading input and proceeds to general physical execution |
| batch/native vector | Compiles the largest contiguous supported subtree and executes expressions and operators with column/batch kernels | Crosses boundaries through the row/batch bridge; expression compile misses fall back to the legacy path |
| Row-based execution | The full semantic oracle and the baseline for all supported physical shapes | Genuinely unsupported SQL/nodes return explicit errors |

`MYDB_VECTORIZED=off|on|auto` controls kernel selection without changing the
physical plan topology. The unified batch protocol is always available; mode
differences mainly determine whether a node internally uses scalar or native
vector kernels. Expressions without a compiled implementation use scalar
evaluation before consuming input; I/O, evaluation, corruption, or
quota errors that occur after input is consumed must be propagated as-is.

### 11.3 Streaming and Blocking

Seq/Zone Map scans, ordinary Filter/Projection/Limit, some Hash Join chains,
unordered parallel scan Exchanges, and external Sort merges can feed upstream
incrementally through bounded row batches. LIMIT stops early
through the pull model and propagates closure to producers.

The following operators usually need to materialize all or part of their
state: Hash Join build, Hash Aggregate, hash DISTINCT, window partitioning,
set deduplication, materialized CTEs, Sort runs, and Top-N heaps. Blocking
does not mean all data must stay in memory; after obtaining a work-area
reservation, spillable operators produce temporary runs/partitions at the soft
watermark and clean them up when execution finishes or is cancelled.

### 11.4 Memory, Cancellation, and Error Contracts

- Memory budgets account for reserved capacity, including unused buffer space.
- Optional caches are skipped or evicted when there is no budget; spillable
  operators spill; unreclaimable allocation failures return MySQL 1037/HY001.
- Long loops check the cancellation token and the absolute deadline at least
  once per batch.
- A consumer that exits early must close the channel, cancel, and join
  producers; no detached worker may be left behind.
- Temporary files, workers, reservations, and per-node charges must return to
  zero on every path: success, error, cancellation, and early drop.
- Unsupported SQL/protocols return clear errors; errors must never be turned
  into NULLs, empty results, or silent no-ops.

For execution modes, cancellation, and spill behavior, see
[SELECT Execution](executor.md).

## 12. How to Read and Diagnose Plans

### 12.1 EXPLAIN Formats

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

### 12.2 Optimizer Trace

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

### 12.3 Common Troubleshooting Order

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

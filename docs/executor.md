# SELECT Execution

This document explains query execution modes, data access, resource limits,
and cancellation in mydb. For plan selection, operator costs, and reading
EXPLAIN output, see [Optimizer and Execution Operators](optimizer-and-operators.md).

## 1. From Plan to Execution

mydb caches reusable query plans and binds parameter values for each execution.
The physical plan determines scan types, join order, join algorithms, and
parallel topology. Current memory pressure, cache residency, and concurrent
load may adjust batch sizes, prefetching, and spill behavior within an operator,
but do not change those plan decisions.

| Execution mode | Typical use | Fallback behavior |
| --- | --- | --- |
| Compiled fast path | Single-table unique point lookups, index range/multi lookups, fused scan/filter/projection/LIMIT, and supported range aggregates | Other query shapes use general physical execution |
| Row-based execution | Supported queries that require scalar processing | Unsupported SQL returns an explicit error |
| Batch/vector execution | Contiguous groups of supported operators and expressions | Scalar evaluation is used where vector processing is unavailable |
| Parallel execution | Eligible scans, joins, aggregation, and sorting | Degree of parallelism and ordering requirements constrain worker use |

All execution modes preserve SQL values, NULL handling, errors, exact decimal
arithmetic, time-zone rules, and collation semantics. Selecting another mode
does not enable otherwise unsupported SQL.

## 2. Scans and Data Access

Supported access paths include sequential heap scans, primary and secondary
index point/range scans, covering access, index fast full scans, multi-range
reads, full-text search, spatial search, vector search, and Zone Map scans.

- Zone Maps select candidate page ranges; the original row predicate is still
  evaluated. Untrusted summaries or insufficient optional memory cause a safe
  fallback to an ordinary scan.
- Index access respects heap MVCC visibility. A covering index does not bypass
  transaction visibility checks.
- Parallel index lookups preserve the ordering required by the physical plan,
  even if storage reads complete in a different order.
- Partition pruning is consistent across all workers of a statement.
- `FOR SYSTEM_TIME AS OF` validates and registers a historical snapshot before
  execution. Missing or degraded historical indexes fall back to heap/undo
  access when the required history is available.

## 3. Errors and Resource Limits

Unsupported SQL returns an explicit error. Evaluation failures, I/O errors,
corruption, and quota failures are reported rather than converted into NULLs
or empty results.

Query memory is governed by shared and per-query budgets. Spillable operators
can move intermediate data to temporary files at a memory soft watermark.
When required memory cannot be admitted, the query returns MySQL error
1037/HY001. Temporary-disk quotas can also cause an explicit error.

See [Server Limits](limits.md) and [Configuration](configuration.md) for
budgets and configurable ranges.

## 4. Statement Context and Streams

All workers of a statement share its parameters, session settings, snapshot,
partition pruning, cancellation, deadline, and resource budgets. Streaming
operators deliver bounded batches so that results can be consumed incrementally.

A query that finishes, fails, reaches a limit, or is cancelled releases its
workers, temporary files, and memory reservations. Cancellation and timeouts
are checked during execution; they do not leave detached query work running.

## 5. Vectorization Boundaries

`MYDB_VECTORIZED=off|on|auto` controls scalar versus vector processing without
changing the physical plan topology:

- `off` uses scalar expression and aggregate processing within the batch
  pipeline. It does not force every query through a row-based executor.
- `on` and `auto` use typed vector processing where supported and scalar
  processing for other expressions.
- Queries can cross row/batch boundaries as required by their operators.

Results and SQL semantics remain consistent across these modes. To inspect
the chosen operators and runtime behavior, use `EXPLAIN` or `EXPLAIN ANALYZE`.

## 6. Columns and Runtime Statistics

Qualified and unqualified column names follow the same resolution rules
across execution modes. Ambiguous references return errors. Aliases retain
their SQL scope and do not change meaning when execution is parallelized.

`EXPLAIN ANALYZE` attributes runtime statistics to the corresponding physical
plan nodes. Compare estimated and actual rows to identify expensive scans,
join fanout, and unexpected materialization.

## 7. Blocking Operators and Spill

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
for spill limits and [Optimizer and Execution Operators](optimizer-and-operators.md)
for plan diagnostics.

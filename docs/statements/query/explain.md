# `EXPLAIN`

## Description

Shows the physical plan of a SELECT or DML statement. `EXPLAIN ANALYZE` actually executes the target and returns observed row counts and timings.

## Syntax

```ebnf
<explain> ::= 'EXPLAIN' [ 'ANALYZE' ]
              [ 'FORMAT' [ '=' ] ( 'TRADITIONAL' | 'TREE' | 'JSON' ) ]
              <explainable-statement>
<explainable-statement> ::= <select-statement> | <insert-statement>
                            | <update-statement> | <delete-statement>
                            | <table-query>
```

## Behavior and Limitations

- Plain `EXPLAIN` does not execute the target; `ANALYZE` does, so DML may modify data.
- When no format is specified explicitly, plain EXPLAIN uses the session `explain_format`; ANALYZE defaults to TREE.
- The output includes the scan method, indexes, join order, estimated rows, and operator details.
  ANALYZE's TREE follows the MySQL iterator convention: `actual time=first..last` is the
  average time to the first result and to completion per loop, `rows` is the average output
  row count per loop, and `loops` is the real number of opens. Nodes never opened show
  `never executed`; nodes fused by the compiled executor show `included in operator #N`
  and do not duplicate parent-node statistics.
- The external-memory sort nodes of set operations additionally show `peak_mem`, `spill_bytes`,
  `spill_partitions`, `spill_file_opens`, `spill_repartitions`,
  `spill_max_depth`, and `spill_passes`. A derived set operation inside WITH/CTE shows
  the full physical boundary; when a correctness fallback re-plans it, statistics are
  aggregated up to that visible boundary, and its child nodes are explicitly marked as fused.
- The mydb-extended JSON provides `rows_total`, `active_time_total_ms`,
  `self_time_total_ms`, and operator-self I/O per node; the query-level `execution`
  provides wall time, total I/O, and unassigned I/O.
- TREE shows the query-level `physical_reads`, `cache_reads`, and
  `cache_hit_rate` (including zero values) on a separate leading `Query I/O:` line.
  Non-zero node I/O appears in each node's own actual statistics, using
  `io_scope=operator_self` for the node itself, or
  `io_scope=operator_interval_inclusive` for reads within the execution interval
  (child nodes included). Query totals and node statistics cannot be added together, and
  inclusive nodes may also overlap; even when the root node equals the query total, both
  scopes are retained for diagnosis.
- `physical_reads` is the number of buffer-pool-miss reads, which may still hit the
  operating-system cache; `cache_reads` is the number of buffer-pool hits. Neither is a
  row count or a deduplicated page count, and both accumulate over all loops.
  The hit rate is `cache_reads / (cache_reads + physical_reads)`; a query with zero reads
  shows `0.00%`. Reads that cannot be safely attributed to a specific operator are shown
  separately as `unassigned_physical_reads` / `unassigned_cache_reads` and are never
  disguised as operator-self I/O.
  DML likewise uses the query summary line and does not misattribute reads produced by
  triggers, constraints, or the write path to static candidate access nodes. JSON continues
  to provide complete query and node statistics.
- The mydb-extended TRADITIONAL ANALYZE appends `actual_rows`, `actual_loops`,
  `actual_first_row_ms`, `actual_last_row_ms`, and `actual_rows_total` after the
  standard 12 columns.
- Uses the same privileges, object resolution, session semantics, CBO, hints, and plan-cache rules as the real statement.

## Examples

```sql
EXPLAIN SELECT * FROM orders WHERE customer_id = 42;
EXPLAIN FORMAT=JSON SELECT * FROM orders ORDER BY created_at DESC LIMIT 10;
EXPLAIN ANALYZE SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id;
```

The third statement really scans and aggregates data; do not wrap write statements that you do not want executed in `EXPLAIN ANALYZE`.

## Related Statements

- [SELECT](select.md)
- [EXPLAIN FOR CONNECTION](explain-for-connection.md)

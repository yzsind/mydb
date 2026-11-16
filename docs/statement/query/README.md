# Query Statements

| Statement | Description |
| --- | --- |
| [SELECT](select.md) | Queries, CTEs, set operations, grouping, windows, and locking reads |
| [SELECT INTO variables](select-into-variables.md) | Assigns a single-row query result to user variables |
| [SELECT INTO OUTFILE / DUMPFILE](select-into-file.md) | Server-side file export |
| [EXPLAIN](explain.md) | Views, or actually executes and analyzes, a plan |
| [EXPLAIN FOR CONNECTION](explain-for-connection.md) | Shows the plan another connection is currently executing |
| [DESCRIBE](describe.md) | Returns the column definitions of a table |
| [DO](do.md) | Evaluates expressions and discards the results |

Clause guides: [TABLE/VALUES, joins, grouping and CTEs](select.md#syntax), [sampling](select.md#table-sampling), [historical reads](../../mysql80-compatibility.md#time-travel-queries), [window functions](../../function/aggregate-window/README.md), [locks and isolation](../../mvcc-and-locking.md). EXPLAIN ANALYZE executes its target, including writes. Use the [shared example schema](../examples.md).

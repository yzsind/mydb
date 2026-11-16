# `LAST_INSERT_ID`

## Description

Returns the most recently generated auto-increment value for the current connection; can also set that session value.

## Syntax

```sql
LAST_INSERT_ID()
LAST_INSERT_ID(expr)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | No | Numeric or integer | Input expression. |

## Return Value

With no argument, returns the connection's recorded first automatically generated ID from its most recent relevant insert. LAST_INSERT_ID(expr) stores/returns an unsigned session value; a NULL expression returns SQL NULL while storing zero. Read the value before another modifying statement changes that state.

## Examples

```sql
SELECT LAST_INSERT_ID();
```

Result: returns the most recently recorded auto-increment value for the current connection.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

- Multirow inserts record the first generated ID, not necessarily the last row's ID. The explicit setter changes session state. A manual rollback does not restore that state; do not infer sequence-gap or failed-insert behavior from it. Use explicit AS aliases for the result column when needed.

## Related Functions

[`ROW_COUNT`](row_count.md), [`FOUND_ROWS`](found_rows.md).

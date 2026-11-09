# `ROW_COUNT`

## Description

Returns the affected-row count recorded for the preceding statement.

## Syntax

```sql
ROW_COUNT()
```

## Parameters

This function takes no arguments.

## Return Value

Returns a signed integer: DML reports its affected-row count, while a result-producing SELECT leaves -1. Read it immediately after the statement of interest.

## Examples

```sql
DO 1;
SELECT ROW_COUNT();
```

Result:

```text
0
```

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

- Read immediately after the statement of interest. UPDATE affected counts depend on CLIENT_FOUND_ROWS (matched versus changed rows); result-set statements leave -1. DDL/DO commonly leave 0; do not interpret ROW_COUNT as the current SELECT cardinality.

## Related Functions

[`FOUND_ROWS`](found_rows.md), [`LAST_INSERT_ID`](last_insert_id.md).

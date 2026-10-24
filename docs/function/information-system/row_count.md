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
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

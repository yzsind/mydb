# `DATABASE`

## Description

Returns the current default database.

## Syntax

```sql
DATABASE()
```

## Parameters

This function takes no arguments.

## Return Value

Returns text describing the current session database, or SQL NULL when none is selected.

## Examples

```sql
SELECT DATABASE();
```

Result: With the example connection selecting the `test` database, returns the string `test`. A connection with no selected database returns SQL NULL.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

[`SCHEMA`](schema.md).

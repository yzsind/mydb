# `CONNECTION_ID`

## Description

Returns the thread ID of the current connection.

## Syntax

```sql
CONNECTION_ID()
```

## Parameters

This function takes no arguments.

## Return Value

Returns the thread ID of the current connection. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT CONNECTION_ID();
```

Result: returns the unsigned connection ID of the current connection.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

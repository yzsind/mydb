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
| `value` | No | Numeric or integer | The 1st argument; the input value |

## Return Value

Returns the most recently generated auto-increment value for the current connection; can also set that session value. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT LAST_INSERT_ID();
```

Result: returns the most recently recorded auto-increment value for the current connection.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

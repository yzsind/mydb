# `RAND`

## Description

Returns a pseudorandom number in the range `[0, 1)`, with an optional seed.

## Syntax

```sql
RAND([seed])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | No | Numeric or integer | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns an integer, fixed-point, or double-precision value; invalid domain, overflow, and `NULL` are handled according to the current SQL mode.

## Examples

```sql
SELECT RAND(7);
```

Result: returns a `[0, 1)` value from a reproducible pseudorandom sequence.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

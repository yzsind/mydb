# `ABS`

## Description

Returns the absolute value of a number.

## Syntax

```sql
ABS(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric or integer | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns an integer, fixed-point, or double-precision value; invalid domain, overflow, and `NULL` are handled according to the current SQL mode.

## Examples

```sql
SELECT ABS(-12);
```

Result: 12

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

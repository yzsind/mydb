# `ATAN2`

## Description

Returns the quadrant-aware arc tangent from two coordinate arguments.

## Syntax

```sql
ATAN2(y, x)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `y` | Yes | Numeric expression | The 1st argument; the meaning is determined by the function syntax and description |
| `x` | Yes | Numeric expression | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a double-precision numeric value; SQL NULL input returns NULL. Invalid real-valued domains and overflow produce the applicable numeric diagnostic; an invalid domain can return NULL.

## Examples

```sql
SELECT ATAN2(4, 2);
```

Result:

```text
1.1071487177940904
```

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

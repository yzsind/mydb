# `SQRT`

## Description

Returns the square root of a non-negative number.

## Syntax

```sql
SQRT(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric or integer | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a double-precision numeric value; SQL NULL input returns NULL. Invalid real-valued domains and overflow produce the applicable numeric diagnostic; an invalid domain can return NULL.

## Examples

```sql
SELECT SQRT(2);
```

Result:

```text
1.4142135623730951
```

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

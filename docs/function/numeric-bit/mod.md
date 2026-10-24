# `MOD`

## Description

Returns the remainder of a division.

## Syntax

```sql
MOD(dividend, divisor)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `dividend` | Yes | Numeric expression | The 1st argument; the meaning is determined by the function syntax and description |
| `divisor` | Yes | Numeric expression | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns the remainder in the derived numeric domain. SQL NULL input or a zero divisor returns NULL; division-by-zero diagnostics follow sql_mode.

## Examples

```sql
SELECT MOD(4, 2);
```

Result:

```text
0
```

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

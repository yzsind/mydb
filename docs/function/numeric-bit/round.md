# `ROUND`

## Description

Rounds a number to the specified number of decimal places.

## Syntax

```sql
ROUND(number [, decimals])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric or integer | The 1st argument; the meaning is determined by the function syntax and description |
| `decimals` | No | Numeric or integer | The 2nd argument; the number of decimal places to keep or process |

## Return Value

Returns the number rounded to the requested decimal places (default 0), preserving the derived exact/approximate numeric domain. Negative decimal places round to tens, hundreds, and so on; SQL NULL input returns NULL. Use TRUNCATE when fractional digits should be discarded without rounding.

## Examples

```sql
SELECT ROUND(2);
```

Result:

```text
2
```

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

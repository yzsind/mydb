# `FORMAT`

## Description

Formats a number with a given number of decimals and an optional locale.

## Syntax

```sql
FORMAT(number, decimals [, locale])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric or integer | The 1st argument; the meaning is determined by the function syntax and description |
| `decimals` | Yes | Numeric or integer | The 2nd argument; the number of decimal places to keep or process |
| `locale` | No | String or binary string | The 3rd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT FORMAT(1234.5, 2);
```

Result: 1,234.50

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

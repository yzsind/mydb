# `EXPORT_SET`

## Description

Selects the on/off strings according to an integer bitmap and concatenates the result.

## Syntax

```sql
EXPORT_SET(bits, on, off [, separator [, number_of_bits]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `bits` | Yes | Integer | The 1st argument; the meaning is determined by the function syntax and description |
| `on` | Yes | String or binary string | The 2nd argument; the meaning is determined by the function syntax and description |
| `off` | Yes | String or binary string | The 3rd argument; the meaning is determined by the function syntax and description |
| `separator` | No | String or binary string | The 4th argument; the separator used in the concatenated result |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT EXPORT_SET(5, 'Y', 'N', ',', 3);
```

Result: Y,N,Y

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

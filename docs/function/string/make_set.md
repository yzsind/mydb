# `MAKE_SET`

## Description

Selects arguments according to a bitmap and joins them into a comma-separated string.

## Syntax

```sql
MAKE_SET(bits, str1 [, str2] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `bits` | Yes | Integer | The 1st argument; the meaning is determined by the function syntax and description |
| `str1 ...` | No, repeatable | String | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT MAKE_SET(5, 'a', 'b', 'c');
```

Result: a,c

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

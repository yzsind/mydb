# `CONCAT`

## Description

Concatenates all arguments in order.

## Syntax

```sql
CONCAT(str1 [, str2 [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str1` | Yes | String | The 1st argument; the meaning is determined by the function syntax and description |
| `str2 ...` | No, repeatable | String | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT CONCAT('my', 'db');
```

Result: mydb

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

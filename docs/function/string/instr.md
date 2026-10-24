# `INSTR`

## Description

Returns the position of the first occurrence of a substring within a string.

## Syntax

```sql
INSTR(str, substr)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |
| `substr` | Yes | String | The 2nd argument; the substring to search for |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT INSTR('mydb', 'db');
```

Result: 3

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

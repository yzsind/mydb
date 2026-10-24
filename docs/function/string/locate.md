# `LOCATE`

## Description

Returns the position of a substring within a string, optionally from a given starting position.

## Syntax

```sql
LOCATE(substr, str [, pos])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `substr` | Yes | String | The 1st argument; the substring to search for |
| `str` | Yes | String | The 2nd argument; The string to process |
| `pos` | No | Numeric or integer | The 3rd argument; the 1-based position |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT LOCATE('db', 'mydb');
```

Result: 3

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

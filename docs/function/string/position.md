# `POSITION`

## Description

Returns the position of the first occurrence of a substring within a string.

## Syntax

```sql
POSITION(substr IN str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `substr` | Yes | String | The substring to search for |
| `str` | Yes | String | The string to be searched |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT POSITION('db' IN 'mydb');
```

Result: 3

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

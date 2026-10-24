# `TRIM`

## Description

Removes spaces or a given character sequence from both ends or a specified direction of a string.

## Syntax

```sql
TRIM([[BOTH | LEADING | TRAILING] [remstr] FROM] str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The string to trim |
| `direction` | No | BOTH, LEADING, or TRAILING | The trimming direction; defaults to BOTH |
| `remstr` | No | String | The content to remove; defaults to spaces |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT TRIM(BOTH 'x' FROM 'xmydbx');
```

Result: mydb

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

# `LOWER`

## Description

Converts a string to lowercase.

## Syntax

```sql
LOWER(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT LOWER('MyDB');
```

Result: mydb

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

- [`LCASE`](lcase.md): alias of this function.

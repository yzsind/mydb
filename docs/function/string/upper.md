# `UPPER`

## Description

Converts a string to uppercase.

## Syntax

```sql
UPPER(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT UPPER('mydb');
```

Result: MYDB

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

- [`UCASE`](ucase.md): alias of this function.

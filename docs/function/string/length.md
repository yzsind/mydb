# `LENGTH`

## Description

Returns the length of a string in bytes.

## Syntax

```sql
LENGTH(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT LENGTH('mydb');
```

Result: 4

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

- [`OCTET_LENGTH`](octet_length.md): alias of this function.

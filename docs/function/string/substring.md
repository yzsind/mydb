# `SUBSTRING`

## Description

Extracts a substring from a specified position, optionally with a given length.

## Syntax

```sql
SUBSTRING(str, pos)
SUBSTRING(str, pos, len)
SUBSTRING(str FROM pos [FOR len])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |
| `pos` | Yes | Numeric or integer | The 2nd argument; the 1-based position |
| `len` | No | Integer | The 3rd argument; the length, interpreted in the function's character or byte semantics |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT SUBSTRING('mydb', 3, 2);
```

Result: db

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

- [`MID`](mid.md), [`SUBSTR`](substr.md): aliases of this function.

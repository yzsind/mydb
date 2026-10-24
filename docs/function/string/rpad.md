# `RPAD`

## Description

Right-pads a string to a specified length.

## Syntax

```sql
RPAD(str, len, padstr)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |
| `len` | Yes | Integer | The 2nd argument; the length, interpreted with the function's character or byte semantics |
| `padstr` | Yes | String | The 3rd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns text, or binary bytes when binary input semantics apply; it does not return an integer. SQL NULL propagation follows the argument rules below.

## Examples

```sql
SELECT RPAD('mydb', 6, '0');
```

Result:

```text
mydb00
```

## Notes

- All three arguments are required, even when `len` would truncate the input; missing `padstr` returns 1582/42000.
- A NULL argument returns SQL NULL. A negative length returns NULL; an empty pad string returns an empty string when extension would be needed.

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

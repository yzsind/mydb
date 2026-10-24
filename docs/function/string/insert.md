# `INSERT`

## Description

Replaces a portion of a string starting at a specified position.

## Syntax

```sql
INSERT(str, pos, len, newstr)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |
| `pos` | Yes | Numeric or integer | The 2nd argument; the 1-based position |
| `len` | Yes | Integer | The 3rd argument; the length, interpreted in the function's character or byte semantics |
| `newstr` | Yes | String | The 4th argument; the meaning is determined by the function syntax and description |

## Return Value

Returns text, or binary bytes when binary input semantics apply; it does not return an integer. SQL NULL propagation follows the argument rules below.

## Examples

```sql
SELECT INSERT('mydb', 2, 2, 'x');
```

Result:

```text
mxb
```

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

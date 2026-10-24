# `RIGHT`

## Description

Returns the rightmost given number of characters of a string.

## Syntax

```sql
RIGHT(str, len)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |
| `len` | Yes | Integer | The 2nd argument; the length, interpreted with the function's character or byte semantics |

## Return Value

Returns text, or binary bytes when binary input semantics apply; it does not return an integer. SQL NULL propagation follows the argument rules below.

## Examples

```sql
SELECT RIGHT('mydb', 2);
```

Result:

```text
db
```

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

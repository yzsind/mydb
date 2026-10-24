# `REPLACE`

## Description

Replaces all occurrences of a given substring within a string with new content.

## Syntax

```sql
REPLACE(str, from_str, to_str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |
| `from_str` | Yes | String | The 2nd argument; the meaning is determined by the function syntax and description |
| `to_str` | Yes | String | The 3rd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns text, or binary bytes when binary input semantics apply; it does not return an integer. SQL NULL propagation follows the argument rules below.

## Examples

```sql
SELECT REPLACE('mydb', 'db', 'x');
```

Result:

```text
myx
```

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

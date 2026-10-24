# `REPEAT`

## Description

Repeats a string a given number of times.

## Syntax

```sql
REPEAT(str, count)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |
| `count` | Yes | Integer | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns text, or binary bytes when binary input semantics apply; it does not return an integer. SQL NULL propagation follows the argument rules below.

## Examples

```sql
SELECT REPEAT('mydb', 2);
```

Result:

```text
mydbmydb
```

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

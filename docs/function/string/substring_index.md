# `SUBSTRING_INDEX`

## Description

Returns one side of a string split by a delimiter occurrence count.

## Syntax

```sql
SUBSTRING_INDEX(str, delimiter, count)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |
| `delimiter` | Yes | String or binary string | The 2nd argument; the meaning is determined by the function syntax and description |
| `count` | Yes | Integer | The 3rd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns text, or binary bytes when binary input semantics apply; it does not return an integer. SQL NULL propagation follows the argument rules below.

## Examples

```sql
SELECT SUBSTRING_INDEX('mydb', ',', 2);
```

Result:

```text
mydb
```

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

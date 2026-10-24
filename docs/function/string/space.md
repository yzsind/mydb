# `SPACE`

## Description

Returns a string of a given number of spaces.

## Syntax

```sql
SPACE(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |

## Return Value

Returns text, or binary bytes when binary input semantics apply; it does not return an integer. SQL NULL propagation follows the argument rules below.

## Examples

```sql
SELECT LENGTH(SPACE(3)), HEX(SPACE(3));
```

Result:

```text
3	202020
```

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

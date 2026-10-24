# `MASK_INNER`

## Description

Masks the middle characters of a string while keeping both ends.

## Syntax

```sql
MASK_INNER(str, margin1, margin2 [, mask_char])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |
| `margin1` | Yes | Integer | The 2nd argument; the meaning is determined by the function syntax and description |
| `margin2` | Yes | Integer | The 3rd argument; the meaning is determined by the function syntax and description |
| `mask_char` | No | SQL expression matching the function semantics | The 4th argument; the meaning is determined by the function syntax and description |

## Return Value

Masks the middle characters of a string while keeping both ends. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT MASK_INNER('1234567890', 2, 2);
```

Result: 12XXXXXX90

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

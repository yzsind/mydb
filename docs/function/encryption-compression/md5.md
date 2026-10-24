# `MD5`

## Description

Returns the MD5 hexadecimal digest of the input.

## Syntax

```sql
MD5(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Numeric or integer | The 1st argument; the input value |

## Return Value

Returns the MD5 hexadecimal digest of the input. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT MD5('mydb');
```

Result: returns the MD5 digest as 32 hexadecimal characters.

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

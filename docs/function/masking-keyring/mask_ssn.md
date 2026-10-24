# `MASK_SSN`

## Description

Masks digits according to social security number rules.

## Syntax

```sql
MASK_SSN(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |

## Return Value

Masks digits according to social security number rules. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT MASK_SSN('123456789');
```

Result: XXXXX6789

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

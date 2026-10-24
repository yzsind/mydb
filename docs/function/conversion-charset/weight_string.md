# `WEIGHT_STRING`

## Description

Returns the comparison weight string of a string under its collation.

## Syntax

```sql
WEIGHT_STRING(str [AS {CHAR|BINARY}(N)] [LEVEL level_list])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Numeric or integer | The 1st argument; the input value |

## Return Value

Returns the comparison weight string of a string under its collation. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT HEX(WEIGHT_STRING('A'));
```

Result: returns the hexadecimal text of the weight bytes the collation uses to compare `A`.

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

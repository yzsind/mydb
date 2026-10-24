# `CAST`

## Description

Converts an expression to the specified SQL data type.

## Syntax

```sql
CAST(expr AS type)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any expression | The value to convert |
| `type` | Yes | Supported CAST target type | The target data type with optional length, precision, or charset |

## Return Value

Converts an expression to the specified SQL data type. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT CAST('42' AS SIGNED);
```

Result: 42

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

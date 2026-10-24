# `COERCIBILITY`

## Description

Returns the collation coercibility value of a string expression.

## Syntax

```sql
COERCIBILITY(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Numeric or integer | The 1st argument; the input value |

## Return Value

Returns an integer coercibility level used in collation resolution; numeric expressions have level 5.

## Examples

```sql
SELECT COERCIBILITY(2);
```

Result:

```text
5
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

# `COLLATE`

## Description

Explicitly assigns a collation to an expression.

## Syntax

```sql
expr COLLATE collation_name
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Character expression | The expression to which the collation is assigned |
| `collation_name` | Yes | Collation name | A collation compatible with the expression's charset |

## Return Value

Explicitly assigns a collation to an expression. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT 'A' COLLATE utf8mb4_bin = 'a';
```

Result: 0

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

# `GROUPING`

## Description

Indicates whether the expression in a result row was produced by `ROLLUP`.

## Syntax

```sql
GROUPING(expr [, expr] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes, repeatable | Grouping expression | Indicates whether the expression at that position was produced by ROLLUP |

## Return Value

Returns an integer bit mask identifying expressions replaced by super-aggregate NULLs in ROLLUP/CUBE/GROUPING SETS; an ordinary data NULL does not set the bit. For one argument, regular rows return 0 and rolled-up rows return 1. This is a grouping indicator, not a window function.

## Examples

```sql
SELECT v, GROUPING(v) FROM (SELECT 1 AS v) t GROUP BY v WITH ROLLUP;
```

Result:

```text
1	0
NULL	1
```

## Notes

- GROUPING tests a super-aggregate marker, not whether the displayed value is NULL. Its arguments must correspond to grouping expressions.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

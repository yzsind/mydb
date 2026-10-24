# `COUNT`

## Description

Returns the number of rows or non-`NULL` values in the group; also usable as a window aggregate function.

## Syntax

```sql
COUNT(*)
COUNT(expr)
COUNT(DISTINCT expr [, expr ...])
COUNT(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | No | Expression or * | Counts rows or non-NULL values |
| `DISTINCT` | No | Keyword | Deduplicates by one or more expressions |
| `window_spec` | No | Window definition | When OVER is used, defines the partition, ordering, and frame |

## Return Value

Returns a BIGINT count. COUNT(*) includes every row; COUNT(expr) ignores SQL NULL. COUNT(DISTINCT a,b,...) excludes rows with any NULL key. Empty input returns 0. The OVER form counts within its frame.

## Examples

```sql
SELECT COUNT(*) FROM (SELECT 1 UNION ALL SELECT 2) AS t;
```

Result: 2

## Notes

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

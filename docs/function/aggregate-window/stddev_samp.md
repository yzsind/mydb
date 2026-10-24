# `STDDEV_SAMP`

## Description

Returns the sample standard deviation; also usable as a window aggregate function.

## Syntax

```sql
STDDEV_SAMP(expr)
STDDEV_SAMP(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | The 1st argument; the expression to evaluate |

## Return Value

Returns a DOUBLE sample statistic over non-NULL numeric values (standard deviation for STDDEV_SAMP; variance for VAR_SAMP). Fewer than two non-NULL values returns NULL. OVER computes the statistic in its frame.

## Examples

```sql
SELECT STDDEV_SAMP(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
0.7071067811865476
```

## Notes

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

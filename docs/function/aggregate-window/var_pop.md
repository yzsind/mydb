# `VAR_POP`

## Description

Returns the population variance; also usable as a window aggregate function.

## Syntax

```sql
VAR_POP(expr)
VAR_POP(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | The 1st argument; the expression to evaluate |

## Return Value

Returns a DOUBLE population statistic over non-NULL numeric values (standard deviation for STD/STDDEV/STDDEV_POP; variance for VAR_POP/VARIANCE). Empty or all-NULL input returns NULL; one non-NULL value returns 0. OVER computes the statistic in its frame.

## Examples

```sql
SELECT VAR_POP(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
0.25
```

## Notes

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

[`VARIANCE`](variance.md).

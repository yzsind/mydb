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
| `expr` | Yes | Any SQL expression | Expression to evaluate. |

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

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`STDDEV_POP`](stddev_pop.md), [`VAR_POP`](var_pop.md), [`VAR_SAMP`](var_samp.md).

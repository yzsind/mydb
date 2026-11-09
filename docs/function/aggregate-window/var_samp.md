# `VAR_SAMP`

## Description

Returns the sample variance; also usable as a window aggregate function.

## Syntax

```sql
VAR_SAMP(expr)
VAR_SAMP(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | Expression to evaluate. |

## Return Value

Returns a DOUBLE sample statistic over non-NULL numeric values (standard deviation for STDDEV_SAMP; variance for VAR_SAMP). Fewer than two non-NULL values returns NULL. OVER computes the statistic in its frame.

## Examples

```sql
SELECT VAR_SAMP(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
0.5
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`STDDEV_POP`](stddev_pop.md), [`STDDEV_SAMP`](stddev_samp.md), [`VAR_POP`](var_pop.md).

# `STDDEV_POP`

## Description

Returns the population standard deviation; also usable as a window aggregate function.

## Syntax

```sql
STDDEV_POP(expr)
STDDEV_POP(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | Expression to evaluate. |

## Return Value

Returns a DOUBLE population statistic over non-NULL numeric values (standard deviation for STD/STDDEV/STDDEV_POP; variance for VAR_POP/VARIANCE). Empty or all-NULL input returns NULL; one non-NULL value returns 0. OVER computes the statistic in its frame.

## Examples

```sql
SELECT STDDEV_POP(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
0.5
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`STD`](std.md), [`STDDEV`](stddev.md).

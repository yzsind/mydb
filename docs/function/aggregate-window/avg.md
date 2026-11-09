# `AVG`

## Description

Returns the average of non-`NULL` values; also usable as a window aggregate function.

## Syntax

```sql
AVG([DISTINCT] expr)
AVG(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | Expression to evaluate. |

## Return Value

Returns the average of non-NULL inputs. Integer/DECIMAL input uses exact fixed-point accumulation and finalizes at the declared input scale plus div_precision_increment; floating input returns an approximate result. Empty or all-NULL input returns SQL NULL. OVER applies the same rule to its frame.

## Examples

```sql
SELECT AVG(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
1.5000
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`SUM`](sum.md), [`COUNT`](count.md).

# `SUM`

## Description

Returns the sum of non-`NULL` values in the group; also usable as a window aggregate function.

## Syntax

```sql
SUM([DISTINCT] expr)
SUM(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | Expression to evaluate. |

## Return Value

Returns an exact DECIMAL for integer/DECIMAL inputs and an approximate numeric result for floating inputs. Ignores SQL NULL; empty or all-NULL input returns SQL NULL. OVER applies the same accumulation to its frame.

## Examples

```sql
SELECT SUM(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
3
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`AVG`](avg.md), [`COUNT`](count.md).

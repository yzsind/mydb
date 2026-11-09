# `LAST_VALUE`

## Description

Returns the last value in the current window frame.

## Syntax

```sql
LAST_VALUE(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | Expression to evaluate. |

## Return Value

Returns the argument value from the last row of the current frame, preserving its type and NULL. An empty frame returns NULL; the default ordered frame need not extend to the partition end.

## Examples

```sql
SELECT v, LAST_VALUE(v) OVER (ORDER BY v) AS result
FROM (SELECT 10 AS v UNION ALL SELECT 20) AS t;
```

Result:

```text
10	10
20	20
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

- With ORDER BY and no explicit frame, the frame ends at the current peer group, not at the partition end. Use ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when you need the partition's last value.

## Related Functions

[`FIRST_VALUE`](first_value.md), [`NTH_VALUE`](nth_value.md), [`LAG`](lag.md), [`LEAD`](lead.md).

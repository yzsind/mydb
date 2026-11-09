# `FIRST_VALUE`

## Description

Returns the first value in the current window frame.

## Syntax

```sql
FIRST_VALUE(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | Expression to evaluate. |

## Return Value

Returns the argument value from the first row of the current frame, preserving its type and NULL. An empty frame returns NULL.

## Examples

```sql
SELECT v, FIRST_VALUE(v) OVER (ORDER BY v) AS result
FROM (SELECT 10 AS v UNION ALL SELECT 20) AS t;
```

Result:

```text
10	10
20	10
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`LAST_VALUE`](last_value.md), [`NTH_VALUE`](nth_value.md), [`LAG`](lag.md), [`LEAD`](lead.md).

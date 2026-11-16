# `NTH_VALUE`

## Description

Returns the Nth value in the current window frame.

## Syntax

```sql
NTH_VALUE(expr, n) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any expression | Value to read, preserving NULL. |
| `n` | Yes | Positive constant integer | One-based frame position; NULL/zero/negative/nonintegral input is invalid. |

## Return Value

Returns the argument value from the requested one-based row of the current frame. If the frame contains fewer rows, returns NULL; otherwise the value type and NULL are preserved.

## Examples

```sql
SELECT v, NTH_VALUE(v, 2) OVER (ORDER BY v) AS result
FROM (SELECT 10 AS v UNION ALL SELECT 20) AS t;
```

Result:

```text
10	NULL
20	20
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`FIRST_VALUE`](first_value.md), [`LAST_VALUE`](last_value.md), [`LAG`](lag.md), [`LEAD`](lead.md).

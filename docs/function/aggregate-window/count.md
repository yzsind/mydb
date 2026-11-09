# `COUNT`

## Description

Returns the number of rows or non-`NULL` values in the group; also usable as a window aggregate function.

## Syntax

```sql
COUNT(*)
COUNT(expr)
COUNT(DISTINCT expr [, expr ...])
COUNT(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | No | Expression or * | Counts rows or non-NULL values |
| `DISTINCT` | No | Keyword | Deduplicates by one or more expressions |
| `window_spec` | No | Window definition | When OVER is used, defines the partition, ordering, and frame |

## Return Value

Returns a BIGINT count. COUNT(*) includes every row; COUNT(expr) ignores SQL NULL. COUNT(DISTINCT a,b,...) excludes rows with any NULL key. Empty input returns 0. The OVER form counts within its frame.

## Examples

```sql
SELECT COUNT(*) FROM (SELECT 1 UNION ALL SELECT 2) AS t;
```

Result: 2

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`AVG`](avg.md), [`SUM`](sum.md).

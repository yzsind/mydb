# `RANK`

## Description

Returns a peer ranking with gaps within the window partition.

## Syntax

```sql
RANK() OVER (window_spec)
```

## Parameters

This function takes no arguments.

## Return Value

Returns a BIGINT rank starting at 1. Peers receive the same rank and subsequent ranks have gaps.

## Examples

```sql
SELECT id,v,RANK() OVER (ORDER BY v) AS result
FROM (SELECT 1 id,10 v UNION ALL SELECT 2,10 UNION ALL SELECT 3,20) t
ORDER BY id;
```

Result:

```text
1	10	1
2	10	1
3	20	3
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`DENSE_RANK`](dense_rank.md), [`PERCENT_RANK`](percent_rank.md), [`CUME_DIST`](cume_dist.md), [`ROW_NUMBER`](row_number.md).

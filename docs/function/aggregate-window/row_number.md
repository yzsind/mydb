# `ROW_NUMBER`

## Description

Returns the sequential number of a row within its window partition.

## Syntax

```sql
ROW_NUMBER() OVER (window_spec)
```

## Parameters

This function takes no arguments.

## Return Value

Returns a BIGINT row position starting at 1 within the partition. Ties in the ordering still receive different positions; add a unique ordering key for deterministic tie order.

## Examples

```sql
SELECT id,v,ROW_NUMBER() OVER (ORDER BY v,id) AS result
FROM (SELECT 1 id,10 v UNION ALL SELECT 2,10 UNION ALL SELECT 3,20) t
ORDER BY id;
```

Result:

```text
1	10	1
2	10	2
3	20	3
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`RANK`](rank.md), [`DENSE_RANK`](dense_rank.md), [`PERCENT_RANK`](percent_rank.md), [`CUME_DIST`](cume_dist.md).

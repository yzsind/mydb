# `CUME_DIST`

## Description

Returns the cumulative distribution of the current row within its window partition.

## Syntax

```sql
CUME_DIST() OVER (window_spec)
```

## Parameters

This function takes no arguments.

## Return Value

Returns a DOUBLE equal to the number of rows through the current peer group divided by the partition row count.

## Examples

```sql
SELECT id,v,CUME_DIST() OVER (ORDER BY v) AS result
FROM (SELECT 1 id,10 v UNION ALL SELECT 2,10 UNION ALL SELECT 3,20) t
ORDER BY id;
```

Result:

```text
1	10	0.6666666666666666
2	10	0.6666666666666666
3	20	1
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`RANK`](rank.md), [`DENSE_RANK`](dense_rank.md), [`PERCENT_RANK`](percent_rank.md), [`ROW_NUMBER`](row_number.md).

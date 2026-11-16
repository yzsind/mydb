# `NTILE`

## Description

Distributes the rows of a window partition as evenly as possible into a given number of buckets.

## Syntax

```sql
NTILE(buckets) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `buckets` | Yes | Positive constant integer | Number of buckets; NULL, zero, negatives and nonintegral expressions are invalid. |

## Return Value

Returns a BIGINT bucket number starting at 1; partition rows are distributed as evenly as possible, with larger buckets first when the division is uneven.

## Examples

```sql
SELECT v, NTILE(2) OVER (ORDER BY v) AS result
FROM (SELECT 10 AS v UNION ALL SELECT 20) AS t;
```

Result:

```text
10	1
20	2
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`RANK`](rank.md), [`DENSE_RANK`](dense_rank.md), [`PERCENT_RANK`](percent_rank.md), [`CUME_DIST`](cume_dist.md).

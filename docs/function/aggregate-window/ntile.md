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
| `expr` | Yes | Any SQL expression | The 1st argument; the expression to evaluate |

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

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

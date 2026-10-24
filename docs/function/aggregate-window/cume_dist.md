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
SELECT v, CUME_DIST() OVER (ORDER BY v) AS result
FROM (SELECT 10 AS v UNION ALL SELECT 20) AS t ORDER BY v;
```

Result: returns two rows with the corresponding window values in the order of `v`.

## Notes

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

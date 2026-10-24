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
SELECT v, ROW_NUMBER() OVER (ORDER BY v) AS result
FROM (SELECT 10 AS v UNION ALL SELECT 20) AS t ORDER BY v;
```

Result: returns two rows with the corresponding window values in the order of `v`.

## Notes

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

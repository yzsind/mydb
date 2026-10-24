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
SELECT v, RANK() OVER (ORDER BY v) AS result
FROM (SELECT 10 AS v UNION ALL SELECT 20) AS t ORDER BY v;
```

Result: returns two rows with the corresponding window values in the order of `v`.

## Notes

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

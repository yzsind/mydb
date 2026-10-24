# `LAST_VALUE`

## Description

Returns the last value in the current window frame.

## Syntax

```sql
LAST_VALUE(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | The 1st argument; the expression to evaluate |

## Return Value

Returns the argument value from the last row of the current frame, preserving its type and NULL. An empty frame returns NULL; the default ordered frame need not extend to the partition end.

## Examples

```sql
SELECT v, LAST_VALUE(v) OVER (ORDER BY v) AS result
FROM (SELECT 10 AS v UNION ALL SELECT 20) AS t;
```

Result:

```text
10	10
20	20
```

## Notes

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

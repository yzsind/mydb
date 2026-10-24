# `MAX`

## Description

Returns the maximum of non-`NULL` values in the group; also usable as a window aggregate function.

## Syntax

```sql
MAX(expr)
MAX(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | The 1st argument; the expression to evaluate |

## Return Value

Returns the largest non-NULL value, with argument type and collation semantics. Empty or all-NULL input returns SQL NULL. OVER finds the maximum in the frame.

## Examples

```sql
SELECT MAX(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
2
```

## Notes

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

# `AVG`

## Description

Returns the average of non-`NULL` values; also usable as a window aggregate function.

## Syntax

```sql
AVG(expr)
AVG(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | The 1st argument; the expression to evaluate |

## Return Value

Returns the average of non-NULL inputs. Integer/DECIMAL input uses exact fixed-point accumulation and finalizes at the declared input scale plus div_precision_increment; floating input returns an approximate result. Empty or all-NULL input returns SQL NULL. OVER applies the same rule to its frame.

## Examples

```sql
SELECT AVG(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
1.5000
```

## Notes

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

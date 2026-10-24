# `SUM`

## Description

Returns the sum of non-`NULL` values in the group; also usable as a window aggregate function.

## Syntax

```sql
SUM(expr)
SUM(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | The 1st argument; the expression to evaluate |

## Return Value

Returns an exact DECIMAL for integer/DECIMAL inputs and an approximate numeric result for floating inputs. Ignores SQL NULL; empty or all-NULL input returns SQL NULL. OVER applies the same accumulation to its frame.

## Examples

```sql
SELECT SUM(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
3
```

## Notes

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

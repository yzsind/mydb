# `NTH_VALUE`

## Description

Returns the Nth value in the current window frame.

## Syntax

```sql
NTH_VALUE(expr, n) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any expression | The value to read |
| `n` | Yes | Positive integer | 1-based position within the window frame |

## Return Value

Returns the argument value from the requested one-based row of the current frame. If the frame contains fewer rows, returns NULL; otherwise the value type and NULL are preserved.

## Examples

```sql
SELECT v, NTH_VALUE(v, 2) OVER (ORDER BY v) AS result
FROM (SELECT 10 AS v UNION ALL SELECT 20) AS t;
```

Result:

```text
10	NULL
20	20
```

## Notes

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

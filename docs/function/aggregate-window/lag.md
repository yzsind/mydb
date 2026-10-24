# `LAG`

## Description

Returns the value at a specified offset before the current row within its window partition.

## Syntax

```sql
LAG(expr [, offset [, default]]) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any expression | The value to read |
| `offset` | No | Non-negative constant integer | Number of rows to look back; defaults to 1 |
| `default` | No | Constant expression | Returned when the offset is out of range; defaults to NULL |

## Return Value

Returns the argument value from the requested preceding partition row; the window frame does not restrict this lookup. A missing row uses the supplied default, or NULL when omitted. A present SQL NULL is not replaced by the default.

## Examples

```sql
SELECT v, LAG(v) OVER (ORDER BY v) AS result
FROM (SELECT 10 AS v UNION ALL SELECT 20) AS t;
```

Result:

```text
10	NULL
20	10
```

## Notes

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

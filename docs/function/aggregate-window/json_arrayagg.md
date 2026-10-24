# `JSON_ARRAYAGG`

## Description

Aggregates values in the group into a JSON array; also usable as a window aggregate function.

## Syntax

```sql
JSON_ARRAYAGG(expr)
JSON_ARRAYAGG(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | The 1st argument; the expression to evaluate |

## Return Value

Returns a JSON array including a JSON null for every SQL NULL input. No input rows returns SQL NULL, whereas one SQL NULL row returns `[null]`. Element order is unspecified without a defined input/window order. OVER aggregates the current frame.

## Examples

```sql
SELECT JSON_ARRAYAGG(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
[1, 2]
```

```sql
SELECT JSON_ARRAYAGG(v) FROM (SELECT NULL v WHERE FALSE) AS empty_input;
SELECT JSON_ARRAYAGG(v) FROM (SELECT NULL v) AS one_null;
```

Results: SQL NULL, then `[null]`.

## Notes

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

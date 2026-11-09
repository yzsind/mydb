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
| `expr` | Yes | Any SQL expression | Expression to evaluate. |

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

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`JSON_OBJECTAGG`](json_objectagg.md), [`GROUP_CONCAT`](group_concat.md), [`ST_COLLECT`](st_collect.md), [`GROUPING`](grouping.md).

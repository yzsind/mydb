# `GROUP_CONCAT`

## Description

Concatenates non-`NULL` values in the group with an optional ordering and separator.

## Syntax

```sql
GROUP_CONCAT([DISTINCT] expr [, expr ...] [ORDER BY ...] [SEPARATOR str])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes, repeatable | Any expression | Value in the group to concatenate |
| `DISTINCT` | No | Keyword | Removes duplicate values |
| `ORDER BY` | No | Ordering clause | Controls the concatenation order |
| `SEPARATOR` | No | String | Separator; defaults to a comma |

## Return Value

Returns text (or binary text for binary inputs) formed from non-NULL input rows, with the requested ordering, DISTINCT and separator. Empty or all-NULL input returns SQL NULL. Output is bounded by group_concat_max_len; truncation records a warning. It is not a window aggregate.

## Examples

```sql
SELECT GROUP_CONCAT(v ORDER BY v SEPARATOR ':') FROM (SELECT 'b' v UNION ALL SELECT 'a') t;
```

Result: a:b

## Notes

- Use its own ORDER BY for deterministic concatenation order; an outer ORDER BY does not order inputs within a group.

- group_concat_max_len defaults to 1024 bytes and has a minimum of 4. Exceeding it truncates output and records a warning. A row whose concatenated expressions include SQL NULL is skipped. Binary inputs retain binary result semantics; use an internal ORDER BY for deterministic concatenation.

## Related Functions

[`JSON_ARRAYAGG`](json_arrayagg.md), [`JSON_OBJECTAGG`](json_objectagg.md), [`ST_COLLECT`](st_collect.md), [`GROUPING`](grouping.md).

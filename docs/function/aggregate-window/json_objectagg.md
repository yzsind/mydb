# `JSON_OBJECTAGG`

## Description

Aggregates key-value pairs in the group into a JSON object; also usable as a window aggregate function.

## Syntax

```sql
JSON_OBJECTAGG(key, value)
JSON_OBJECTAGG(key, value) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `key` | Yes | Non-NULL expression | Member name of the JSON object |
| `value` | Yes | Any expression | Member value of the JSON object |
| `window_spec` | No | Window definition | When OVER is used, defines the window |

## Return Value

Returns a JSON object from key/value pairs. A NULL key is an error; a NULL value becomes JSON null. Empty input returns SQL NULL. Duplicate keys retain the last value encountered, so an unspecified row order cannot choose a deterministic winner. OVER aggregates the current frame.

## Examples

```sql
SELECT JSON_OBJECTAGG(k, v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
{"a": 1, "b": 2}
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`JSON_ARRAYAGG`](json_arrayagg.md), [`GROUP_CONCAT`](group_concat.md), [`ST_COLLECT`](st_collect.md), [`GROUPING`](grouping.md).

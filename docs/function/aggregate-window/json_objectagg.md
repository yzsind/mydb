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

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

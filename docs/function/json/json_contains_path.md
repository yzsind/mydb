# `JSON_CONTAINS_PATH`

## Description

Determines whether a JSON document contains any or all of the specified paths.

## Syntax

```sql
JSON_CONTAINS_PATH(json_doc, one_or_all, path [, path] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `one_or_all` | Yes | `one` or `all` | Matches any one path or all paths |
| `path` | Yes, repeatable | JSON path | The paths to check |

## Return Value

Returns integer 1 when one/all requested paths exist, 0 otherwise, or SQL NULL for a SQL NULL document/mode/path. A JSON null at a path still counts as existing. The mode must be one or all.

## Examples

```sql
SELECT JSON_CONTAINS_PATH('{"a":1}', 'one', '$.a');
```

Result: 1

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_CONTAINS`](json_contains.md), [`JSON_OVERLAPS`](json_overlaps.md), [`JSON_SEARCH`](json_search.md).

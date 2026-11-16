# `JSON_EXTRACT`

## Description

Returns the values matched by one or more JSON paths.

## Syntax

```sql
JSON_EXTRACT(json_doc, path [, path] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `path` | Yes, repeatable | JSON path | One or more paths to extract |

## Return Value

Returns a JSON value for one scalar match, or a JSON array when multiple paths/matches require wrapping. No matches return SQL NULL; a matched JSON null remains JSON null. SQL NULL document or path returns SQL NULL. Invalid JSON/path raises an error.

## Examples

```sql
SELECT JSON_EXTRACT('{"a":1}', '$.a');
```

Result: 1

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_VALUE`](json_value.md), [`JSON_KEYS`](json_keys.md), [`JSON_LENGTH`](json_length.md), [`JSON_DEPTH`](json_depth.md).

# `JSON_KEYS`

## Description

Returns an array of member names from a JSON object.

## Syntax

```sql
JSON_KEYS(json_doc [, path])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document or valid JSON text | The json document to process |
| `path` | No | string | Server filesystem path to format; not a JSON path. |

## Return Value

Returns a JSON array of the member names of the selected object. A non-object or unmatched path returns SQL NULL; SQL NULL document/path also returns NULL. Keys are not values or recursive descendant names.

## Examples

```sql
SELECT JSON_KEYS('{"a":1,"b":2}');
```

Result: ["a", "b"]

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_EXTRACT`](json_extract.md), [`JSON_VALUE`](json_value.md), [`JSON_LENGTH`](json_length.md), [`JSON_DEPTH`](json_depth.md).

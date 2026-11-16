# `JSON_DEPTH`

## Description

Returns the maximum nesting depth of a JSON document.

## Syntax

```sql
JSON_DEPTH(json_doc)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document or valid JSON text | The json document to process |

## Return Value

Returns maximum JSON nesting depth as an integer. A scalar or empty array/object has depth 1; containing arrays/objects add one level. SQL NULL returns NULL.

## Examples

```sql
SELECT JSON_DEPTH('{"a":[1]}');
```

Result: 3

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_EXTRACT`](json_extract.md), [`JSON_VALUE`](json_value.md), [`JSON_KEYS`](json_keys.md), [`JSON_LENGTH`](json_length.md).

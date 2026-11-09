# `JSON_LENGTH`

## Description

Returns the length of a JSON document or of the value at the specified path.

## Syntax

```sql
JSON_LENGTH(json_doc [, path])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document or valid JSON text | The json document to process |
| `path` | No | string | Server filesystem path to format; not a JSON path. |

## Return Value

Returns an integer: number of array elements, number of object members, or 1 for a scalar. SQL NULL or an unmatched path returns SQL NULL; nesting is not counted recursively.

## Examples

```sql
SELECT JSON_LENGTH('[1,2,3]');
```

Result: 3

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_EXTRACT`](json_extract.md), [`JSON_VALUE`](json_value.md), [`JSON_KEYS`](json_keys.md), [`JSON_DEPTH`](json_depth.md).

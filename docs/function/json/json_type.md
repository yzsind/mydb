# `JSON_TYPE`

## Description

Returns the type name of a JSON value.

## Syntax

```sql
JSON_TYPE(json_doc)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document or valid JSON text | The json document to process |

## Return Value

Returns the JSON type-name string, or SQL NULL for SQL NULL input. A JSON null document returns the text NULL, which is distinct from SQL NULL. Typed SQL decimal and temporal JSON scalars retain their corresponding type names.

## Examples

```sql
SELECT JSON_TYPE('{"a":1}');
```

Result: OBJECT

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_EXTRACT`](json_extract.md), [`JSON_VALUE`](json_value.md), [`JSON_KEYS`](json_keys.md), [`JSON_LENGTH`](json_length.md).

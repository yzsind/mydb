# `JSON_REPLACE`

## Description

Replaces only paths that already exist in a JSON document.

## Syntax

```sql
JSON_REPLACE(json_doc, path, value [, path, value] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `path` | Yes, repeatable | JSON path | Paired with the value that follows |
| `value` | Yes, repeatable | any expression | Replaces the value when the path exists |

## Return Value

Returns a JSON document with values replaced only at paths that already exist. Unmatched paths leave the document unchanged. SQL NULL document or path returns SQL NULL; a SQL NULL value argument becomes JSON null.

## Examples

```sql
SELECT JSON_REPLACE('{"a":1}', '$.a', 2);
```

Result: {"a": 2}

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_SET`](json_set.md), [`JSON_INSERT`](json_insert.md), [`JSON_REMOVE`](json_remove.md), [`JSON_ARRAY_APPEND`](json_array_append.md).

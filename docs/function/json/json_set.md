# `JSON_SET`

## Description

Sets JSON path values, creating the path when it does not exist.

## Syntax

```sql
JSON_SET(json_doc, path, value [, path, value] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `path` | Yes, repeatable | JSON path | Paired with the value that follows |
| `value` | Yes, repeatable | any expression | The value written to the corresponding path |

## Return Value

Returns a JSON document with existing eligible paths replaced and eligible missing members/array positions added. This does not recursively create arbitrary missing parent objects. SQL NULL document or path returns SQL NULL; a SQL NULL value argument becomes JSON null.

## Examples

```sql
SELECT JSON_SET('{"a":1}', '$.b', 2);
```

Result: {"a": 1, "b": 2}

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_INSERT`](json_insert.md), [`JSON_REPLACE`](json_replace.md), [`JSON_REMOVE`](json_remove.md), [`JSON_ARRAY_APPEND`](json_array_append.md).

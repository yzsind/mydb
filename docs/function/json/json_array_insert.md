# `JSON_ARRAY_INSERT`

## Description

Inserts values at the specified array path position.

## Syntax

```sql
JSON_ARRAY_INSERT(json_doc, path, value [, path, value] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `path` | Yes, repeatable | array element path | Paired with the value that follows |
| `value` | Yes, repeatable | any expression | The value to insert |

## Return Value

Returns a JSON document after inserting before each selected array element. Array positions beyond the end insert at the end; unsupported/non-array paths are ignored or rejected according to path validity. SQL NULL document or path returns SQL NULL; a SQL NULL value argument becomes JSON null.

## Examples

```sql
SELECT JSON_ARRAY_INSERT('[1,3]', '$[1]', 2);
```

Result: [1, 2, 3]

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_SET`](json_set.md), [`JSON_INSERT`](json_insert.md), [`JSON_REPLACE`](json_replace.md), [`JSON_REMOVE`](json_remove.md).

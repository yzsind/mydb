# `JSON_ARRAY_APPEND`

## Description

Appends values to the end of the JSON array at the specified path.

## Syntax

```sql
JSON_ARRAY_APPEND(json_doc, path, value [, path, value] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `path` | Yes, repeatable | JSON path | The array path, paired with the value that follows |
| `value` | Yes, repeatable | any expression | The value to append |

## Return Value

Returns a JSON document after appending at each matched path. Scalars/objects selected for append are wrapped into arrays. Paths that match nothing are ignored. SQL NULL document or path returns SQL NULL; a SQL NULL value argument becomes JSON null.

## Examples

```sql
SELECT JSON_ARRAY_APPEND('[1,2]', '$', 3);
```

Result: [1, 2, 3]

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_SET`](json_set.md), [`JSON_INSERT`](json_insert.md), [`JSON_REPLACE`](json_replace.md), [`JSON_REMOVE`](json_remove.md).

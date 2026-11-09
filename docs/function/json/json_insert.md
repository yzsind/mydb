# `JSON_INSERT`

## Description

Inserts values into a JSON document only when the path does not exist.

## Syntax

```sql
JSON_INSERT(json_doc, path, value [, path, value] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `path` | Yes, repeatable | JSON path | Paired with the value that follows |
| `value` | Yes, repeatable | any expression | Written when the path does not exist |

## Return Value

Returns a JSON document with values inserted only at eligible paths that do not already exist. Existing members are preserved; missing intermediate parents are not recursively manufactured. SQL NULL document or path returns SQL NULL; a SQL NULL value argument becomes JSON null.

## Examples

```sql
SELECT JSON_INSERT('{"a":1}', '$.b', 2);
```

Result: {"a": 1, "b": 2}

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_SET`](json_set.md), [`JSON_REPLACE`](json_replace.md), [`JSON_REMOVE`](json_remove.md), [`JSON_ARRAY_APPEND`](json_array_append.md).

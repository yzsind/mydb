# `JSON_REMOVE`

## Description

Removes the specified paths from a JSON document.

## Syntax

```sql
JSON_REMOVE(json_doc, path [, path] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `path` | Yes, repeatable | JSON path | The paths to remove |

## Return Value

Returns a JSON document with each selected member/element removed. Unmatched paths preserve the document; removing the root is rejected. SQL NULL document/path returns SQL NULL.

## Examples

```sql
SELECT JSON_REMOVE('{"a":1,"b":2}', '$.b');
```

Result: {"a": 1}

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_SET`](json_set.md), [`JSON_INSERT`](json_insert.md), [`JSON_REPLACE`](json_replace.md), [`JSON_ARRAY_APPEND`](json_array_append.md).

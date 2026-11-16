# `JSON_PRETTY`

## Description

Returns JSON text in a readable, indented format.

## Syntax

```sql
JSON_PRETTY(json_doc)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document or valid JSON text | The json document to process |

## Return Value

Returns formatted JSON text using two-space indentation. SQL NULL returns NULL; invalid JSON raises an error.

## Examples

```sql
SELECT JSON_PRETTY('{"a":1}');
```

Result:

```text
{
  "a": 1
}
```

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_STORAGE_SIZE`](json_storage_size.md), [`JSON_STORAGE_FREE`](json_storage_free.md).

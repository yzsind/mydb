# `JSON_STORAGE_FREE`

## Description

Validates a JSON document and returns its partial-update free-space compatibility value.

## Syntax

```sql
JSON_STORAGE_FREE(json_doc)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document or valid JSON text | The json document to process |

## Return Value

Returns integer 0 for every valid non-NULL document, and SQL NULL for SQL NULL input. mydb does not track MySQL-style in-place binary-JSON free space; this value is not a measurement of heap or LOB free capacity.

## Examples

```sql
SELECT JSON_STORAGE_FREE('{"a":1}');
```

Result: 0

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_STORAGE_SIZE`](json_storage_size.md), [`JSON_PRETTY`](json_pretty.md).

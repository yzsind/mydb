# `JSON_STORAGE_SIZE`

## Description

Returns the size of a JSON value encoded in MySQL binary JSON format.

## Syntax

```sql
JSON_STORAGE_SIZE(json_doc)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document or valid JSON text | The json document to process |

## Return Value

Returns the MySQL binary-JSON encoded byte count, or SQL NULL for SQL NULL input. It does not report mydb's internal JSONB size, row overhead, LOB allocation, or reusable storage.

## Examples

```sql
SELECT JSON_STORAGE_SIZE('{"a":1}');
```

Result:

```text
13
```

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_STORAGE_FREE`](json_storage_free.md), [`JSON_PRETTY`](json_pretty.md).

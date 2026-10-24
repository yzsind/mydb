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
| `json_doc` | Yes | JSON document or JSON path/scalar | The 1st argument; the JSON document to process |

## Return Value

Returns the MySQL binary-JSON encoded byte count, or SQL NULL for SQL NULL input. It does not report mydb's internal JSONB size, row overhead, LOB allocation, or reusable storage.

## Examples

```sql
SELECT JSON_STORAGE_SIZE('{"a":1}');
```

Result: returns the number of bytes required for binary JSON storage.

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

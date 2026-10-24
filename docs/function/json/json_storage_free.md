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
| `json_doc` | Yes | JSON document or JSON path/scalar | The 1st argument; the JSON document to process |

## Return Value

Returns integer 0 for every valid non-NULL document, and SQL NULL for SQL NULL input. mydb does not track MySQL-style in-place binary-JSON free space; this value is not a measurement of heap or LOB free capacity.

## Examples

```sql
SELECT JSON_STORAGE_FREE('{"a":1}');
```

Result: 0

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

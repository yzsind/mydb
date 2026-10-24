# `JSON_TYPE`

## Description

Returns the type name of a JSON value.

## Syntax

```sql
JSON_TYPE(json_doc)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document or JSON path/scalar | The 1st argument; the JSON document to process |

## Return Value

Returns the JSON type-name string, or SQL NULL for SQL NULL input. A JSON null document returns the text NULL, which is distinct from SQL NULL. Typed SQL decimal and temporal JSON scalars retain their corresponding type names.

## Examples

```sql
SELECT JSON_TYPE('{"a":1}');
```

Result: OBJECT

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

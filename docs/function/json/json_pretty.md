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
| `json_doc` | Yes | JSON document or JSON path/scalar | The 1st argument; the JSON document to process |

## Return Value

Returns a JSON value or JSON text; no match, `NULL` input, or an error-handling clause may produce `NULL`.

## Examples

```sql
SELECT JSON_PRETTY('{"a":1}');
```

Result: returns the JSON text indented with two spaces.

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

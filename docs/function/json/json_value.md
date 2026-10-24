# `JSON_VALUE`

## Description

Extracts and converts a scalar value from a JSON path.

## Syntax

```sql
JSON_VALUE(json_doc, path [RETURNING type] [on_empty] [on_error])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `path` | Yes | constant JSON path | Must locate a single scalar |
| `RETURNING type` | No | supported return types | Defaults to CHAR(512) |
| `ON EMPTY` | No | NULL, ERROR, or DEFAULT | Handling when the path does not match |
| `ON ERROR` | No | NULL, ERROR, or DEFAULT | Handling when the conversion fails |

## Return Value

Returns a JSON value or JSON text; no match, `NULL` input, or an error-handling clause may produce `NULL`.

## Examples

```sql
SELECT JSON_VALUE('{"a":1}', '$.a' RETURNING UNSIGNED);
```

Result: 1

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

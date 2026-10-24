# `JSON_ARRAY_INSERT`

## Description

Inserts values at the specified array path position.

## Syntax

```sql
JSON_ARRAY_INSERT(json_doc, path, value [, path, value] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `path` | Yes, repeatable | array element path | Paired with the value that follows |
| `value` | Yes, repeatable | any expression | The value to insert |

## Return Value

Returns a JSON value or JSON text; no match, `NULL` input, or an error-handling clause may produce `NULL`.

## Examples

```sql
SELECT JSON_ARRAY_INSERT('[1,3]', '$[1]', 2);
```

Result: [1, 2, 3]

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

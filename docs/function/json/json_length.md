# `JSON_LENGTH`

## Description

Returns the length of a JSON document or of the value at the specified path.

## Syntax

```sql
JSON_LENGTH(json_doc [, path])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document or JSON path/scalar | The 1st argument; the JSON document to process |
| `path` | No | string | The 2nd argument; a JSON path expression |

## Return Value

Returns an integer; returns `NULL` when the input is `NULL`.

## Examples

```sql
SELECT JSON_LENGTH('[1,2,3]');
```

Result: 3

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

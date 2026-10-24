# `JSON_KEYS`

## Description

Returns an array of member names from a JSON object.

## Syntax

```sql
JSON_KEYS(json_doc [, path])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document or JSON path/scalar | The 1st argument; the JSON document to process |
| `path` | No | string | The 2nd argument; a JSON path expression |

## Return Value

Returns a JSON value or JSON text; no match, `NULL` input, or an error-handling clause may produce `NULL`.

## Examples

```sql
SELECT JSON_KEYS('{"a":1,"b":2}');
```

Result: ["a", "b"]

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

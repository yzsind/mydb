# `JSON_EXTRACT`

## Description

Returns the values matched by one or more JSON paths.

## Syntax

```sql
JSON_EXTRACT(json_doc, path [, path] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `path` | Yes, repeatable | JSON path | One or more paths to extract |

## Return Value

Returns a JSON value or JSON text; no match, `NULL` input, or an error-handling clause may produce `NULL`.

## Examples

```sql
SELECT JSON_EXTRACT('{"a":1}', '$.a');
```

Result: 1

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

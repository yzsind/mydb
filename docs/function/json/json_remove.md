# `JSON_REMOVE`

## Description

Removes the specified paths from a JSON document.

## Syntax

```sql
JSON_REMOVE(json_doc, path [, path] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `path` | Yes, repeatable | JSON path | The paths to remove |

## Return Value

Returns a JSON value or JSON text; no match, `NULL` input, or an error-handling clause may produce `NULL`.

## Examples

```sql
SELECT JSON_REMOVE('{"a":1,"b":2}', '$.b');
```

Result: {"a": 1}

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

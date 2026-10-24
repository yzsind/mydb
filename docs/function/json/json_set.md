# `JSON_SET`

## Description

Sets JSON path values, creating the path when it does not exist.

## Syntax

```sql
JSON_SET(json_doc, path, value [, path, value] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `path` | Yes, repeatable | JSON path | Paired with the value that follows |
| `value` | Yes, repeatable | any expression | The value written to the corresponding path |

## Return Value

Returns a JSON value or JSON text; no match, `NULL` input, or an error-handling clause may produce `NULL`.

## Examples

```sql
SELECT JSON_SET('{"a":1}', '$.b', 2);
```

Result: {"a": 1, "b": 2}

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

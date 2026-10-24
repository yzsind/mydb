# `JSON_REPLACE`

## Description

Replaces only paths that already exist in a JSON document.

## Syntax

```sql
JSON_REPLACE(json_doc, path, value [, path, value] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `path` | Yes, repeatable | JSON path | Paired with the value that follows |
| `value` | Yes, repeatable | any expression | Replaces the value when the path exists |

## Return Value

Returns a JSON value or JSON text; no match, `NULL` input, or an error-handling clause may produce `NULL`.

## Examples

```sql
SELECT JSON_REPLACE('{"a":1}', '$.a', 2);
```

Result: {"a": 2}

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

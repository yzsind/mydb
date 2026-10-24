# `JSON_CONTAINS_PATH`

## Description

Determines whether a JSON document contains any or all of the specified paths.

## Syntax

```sql
JSON_CONTAINS_PATH(json_doc, one_or_all, path [, path] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `one_or_all` | Yes | `one` or `all` | Matches any one path or all paths |
| `path` | Yes, repeatable | JSON path | The paths to check |

## Return Value

Returns `1` when the condition holds and `0` otherwise; `NULL` input usually returns `NULL`.

## Examples

```sql
SELECT JSON_CONTAINS_PATH('{"a":1}', 'one', '$.a');
```

Result: 1

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

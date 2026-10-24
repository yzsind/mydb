# `JSON_SEARCH`

## Description

Searches a JSON document for a string and returns the matching paths.

## Syntax

```sql
JSON_SEARCH(json_doc, one_or_all, search_str [, escape_char [, path] ...])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The scope of the search |
| `one_or_all` | Yes | `one` or `all` | Returns the first match or all matches |
| `search_str` | Yes | string pattern | A LIKE-style search pattern |
| `escape_char` | No | constant character | The escape character for the pattern |
| `path ...` | No, repeatable | JSON path | Restricts the search to subtrees |

## Return Value

Returns a JSON value or JSON text; no match, `NULL` input, or an error-handling clause may produce `NULL`.

## Examples

```sql
SELECT JSON_SEARCH('{"a":"mydb"}', 'one', 'mydb');
```

Result: "$.a"

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

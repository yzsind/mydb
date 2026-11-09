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

Returns a JSON string path for one match, or a JSON array of paths when all finds multiple distinct matches. No match or a SQL NULL required input returns SQL NULL. Search is for JSON string values using LIKE-style % and _ wildcards, not numeric values.

## Examples

```sql
SELECT JSON_SEARCH('{"a":"mydb"}', 'one', 'mydb');
```

Result: "$.a"

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_CONTAINS`](json_contains.md), [`JSON_CONTAINS_PATH`](json_contains_path.md), [`JSON_OVERLAPS`](json_overlaps.md).

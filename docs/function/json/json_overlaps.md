# `JSON_OVERLAPS`

## Description

Determines whether two JSON documents share any key-value pair or array element.

## Syntax

```sql
JSON_OVERLAPS(json_doc1, json_doc2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc1` | Yes | JSON document | The first document |
| `json_doc2` | Yes | JSON document | The second document |

## Return Value

Returns integer 1 for a common array element, complete object key/value pair, or equal scalar; otherwise 0. SQL NULL returns NULL. Types are compared as JSON values, without string-to-number coercion.

## Examples

```sql
SELECT JSON_OVERLAPS('[1,2]', '[2,3]');
```

Result: 1

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_CONTAINS`](json_contains.md), [`JSON_CONTAINS_PATH`](json_contains_path.md), [`JSON_SEARCH`](json_search.md).

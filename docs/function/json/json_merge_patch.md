# `JSON_MERGE_PATCH`

## Description

Merges JSON documents following RFC 7396 semantics.

## Syntax

```sql
JSON_MERGE_PATCH(json_doc, json_doc [, json_doc] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc1` | Yes | JSON document | The base document |
| `json_doc2` | Yes, repeatable | JSON document | Patch documents applied in order |

## Return Value

Returns a JSON value or JSON text; no match, `NULL` input, or an error-handling clause may produce `NULL`.

## Examples

```sql
SELECT JSON_MERGE_PATCH('{"a":1}', '{"b":2}');
```

Result: {"a": 1, "b": 2}

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

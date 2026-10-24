# `JSON_MERGE_PRESERVE`

## Description

Merges JSON documents while preserving conflicting values.

## Syntax

```sql
JSON_MERGE_PRESERVE(json_doc, json_doc [, json_doc] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc1` | Yes | JSON document | The first document |
| `json_doc2` | Yes, repeatable | JSON document | Merged in order, preserving conflicting values |

## Return Value

Returns a JSON value or JSON text; no match, `NULL` input, or an error-handling clause may produce `NULL`.

## Examples

```sql
SELECT JSON_MERGE_PRESERVE('{"a":1}', '{"a":2}');
```

Result: {"a": [1, 2]}

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.
- `JSON_MERGE` is a deprecated alias; new SQL should use this function name.

## Related Functions

[`JSON_MERGE`](json_merge.md): the deprecated alias of this function.

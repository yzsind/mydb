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

Returns JSON after merging documents left to right. Object conflicts preserve values in arrays; arrays concatenate and scalars can be wrapped into arrays. A SQL NULL document argument returns SQL NULL.

## Examples

```sql
SELECT JSON_MERGE_PRESERVE('{"a":1}', '{"a":2}');
```

Result: {"a": [1, 2]}

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.
- `JSON_MERGE` is a deprecated alias; new SQL should use this function name.

## Related Functions

[`JSON_MERGE`](json_merge.md): the deprecated alias of this function.

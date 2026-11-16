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

Returns JSON after applying documents left to right using merge-patch rules. Object keys with JSON null in a patch are deleted; arrays/scalars replace the target rather than concatenating it. A SQL NULL document argument returns SQL NULL.

## Examples

```sql
SELECT JSON_MERGE_PATCH('{"a":1,"b":[1,2]}','{"a":null,"b":[3]}');
```

Result:

```text
{"b": [3]}
```

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_MERGE_PRESERVE`](json_merge_preserve.md).

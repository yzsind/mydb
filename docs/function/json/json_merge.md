# `JSON_MERGE`

## Description

Merges JSON documents; a deprecated alias of `JSON_MERGE_PRESERVE`. This function shares the same implementation and semantics as [`JSON_MERGE_PRESERVE`](json_merge_preserve.md).

## Syntax

```sql
JSON_MERGE(json_doc, json_doc [, json_doc] ...)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`JSON_MERGE_PRESERVE`](json_merge_preserve.md); refer to the primary function's documentation.

## Examples

```sql
SELECT JSON_MERGE('{"a":1}', '{"a":2}');
```

Result: {"a": [1, 2]}

## Related Functions

- [`JSON_MERGE_PRESERVE`](json_merge_preserve.md): the primary function with full documentation.

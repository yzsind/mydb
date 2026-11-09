# `JSON_CONTAINS`

## Description

Determines whether a JSON document contains a candidate value, optionally restricted to a path.

## Syntax

```sql
JSON_CONTAINS(target, candidate [, path])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `target` | Yes | JSON document or valid JSON text | Document whose containment is tested. |
| `candidate` | Yes | JSON document or valid JSON text | Candidate JSON value; JSON string/number types are distinct. |
| `path` | No | Single-location JSON path | Optional subtree; a missing path returns SQL NULL. |

## Return Value

Returns integer 1 when candidate is contained by target at the optional path, 0 when it is not contained, or SQL NULL for NULL arguments or an unmatched optional path. Comparisons preserve JSON types; a string is not coerced to a number.

## Examples

```sql
SELECT JSON_CONTAINS('[1,2,3]', '2');
```

Result: 1

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_CONTAINS_PATH`](json_contains_path.md), [`JSON_OVERLAPS`](json_overlaps.md), [`JSON_SEARCH`](json_search.md).

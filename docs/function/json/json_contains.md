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
| `target` | Yes | JSON document or JSON path/scalar | The 1st argument; the meaning is determined by the function syntax and description |
| `candidate` | Yes | JSON document or JSON path/scalar | The 2nd argument; the meaning is determined by the function syntax and description |
| `path` | No | string | The 3rd argument; a JSON path expression |

## Return Value

Returns `1` when the condition holds and `0` otherwise; `NULL` input usually returns `NULL`.

## Examples

```sql
SELECT JSON_CONTAINS('[1,2,3]', '2');
```

Result: 1

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

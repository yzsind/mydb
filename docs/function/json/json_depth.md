# `JSON_DEPTH`

## Description

Returns the maximum nesting depth of a JSON document.

## Syntax

```sql
JSON_DEPTH(json_doc)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document or JSON path/scalar | The 1st argument; the JSON document to process |

## Return Value

Returns an integer; returns `NULL` when the input is `NULL`.

## Examples

```sql
SELECT JSON_DEPTH('{"a":[1]}');
```

Result: 3

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

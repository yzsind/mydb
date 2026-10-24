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

Returns `1` when the condition holds and `0` otherwise; `NULL` input usually returns `NULL`.

## Examples

```sql
SELECT JSON_OVERLAPS('[1,2]', '[2,3]');
```

Result: 1

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

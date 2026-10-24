# `JSON_ARRAY`

## Description

Constructs a JSON array from the arguments.

## Syntax

```sql
JSON_ARRAY([value [, value] ...])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | No, repeatable | any expression | Values appended to the array in order |

## Return Value

Returns a JSON array, including JSON null for each SQL NULL argument. With no arguments it returns an empty array; JSON_ARRAY(NULL) returns [null], not SQL NULL.

## Examples

```sql
SELECT JSON_ARRAY(1, 2, 3);
```

Result: [1, 2, 3]

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

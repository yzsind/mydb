# `JSON_OBJECT`

## Description

Constructs a JSON object from key-value arguments.

## Syntax

```sql
JSON_OBJECT([key, value] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `key` | No, repeatable | non-NULL string | The member name |
| `value` | No, repeatable | any expression | The member value paired with the preceding key |

## Return Value

Returns a JSON object from key/value pairs, or an empty object with no arguments. A SQL NULL key is an error; a SQL NULL value becomes JSON null. Duplicate keys retain the last supplied value.

## Examples

```sql
SELECT JSON_OBJECT('a', 1);
```

Result: {"a": 1}

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

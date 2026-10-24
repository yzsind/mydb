# `CONVERT_USING`

## Description

Converts a string using the specified charset.

## Syntax

```sql
CONVERT(expr USING charset_name)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | String or binary string | The content to convert |
| `charset_name` | Yes | Charset name | The target charset |

## Return Value

Converts a string using the specified charset. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT CONVERT('mydb' USING utf8mb4);
```

Result: mydb

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

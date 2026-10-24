# `FROM_BASE64`

## Description

Decodes Base64 text.

## Syntax

```sql
FROM_BASE64(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Numeric or integer | The 1st argument; the input value |

## Return Value

Returns decoded binary bytes; NULL input or invalid Base64 returns NULL. The example uses HEX so the bytes have an unambiguous printable representation.

## Examples

```sql
SELECT HEX(FROM_BASE64('bXlkYg=='));
```

Result:

```text
6D796462
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

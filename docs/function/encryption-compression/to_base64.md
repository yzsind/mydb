# `TO_BASE64`

## Description

Encodes the input bytes as Base64 text.

## Syntax

```sql
TO_BASE64(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Numeric or integer | The 1st argument; the input value |

## Return Value

Returns Base64 text; NULL input returns NULL.

## Examples

```sql
SELECT TO_BASE64('mydb');
```

Result:

```text
bXlkYg==
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

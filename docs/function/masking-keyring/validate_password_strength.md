# `VALIDATE_PASSWORD_STRENGTH`

## Description

Returns a password strength score.

## Syntax

```sql
VALIDATE_PASSWORD_STRENGTH(password)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `password` | Yes | String | The 1st argument; the password to score |

## Return Value

Returns a password strength score. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT VALIDATE_PASSWORD_STRENGTH('A-strong-password-1');
```

Result: returns a strength score from 0 to 100.

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

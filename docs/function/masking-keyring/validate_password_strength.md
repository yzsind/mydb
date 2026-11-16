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
| `password` | Yes | String | Password text to score using the built-in heuristic. |

## Return Value

Returns integer 0, 25, 50, 75 or 100 using the built-in length/character-class heuristic. SQL NULL returns NULL; no password is stored or changed.

## Examples

```sql
SELECT VALIDATE_PASSWORD_STRENGTH('A-strong-password-1');
```

Result:

```text
100
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[Category reference](README.md).

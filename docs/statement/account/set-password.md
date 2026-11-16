# `SET PASSWORD`

## Description

Changes the password of the current account or a specified account, supporting old-password confirmation, random passwords, and dual-credential rotation.

## Syntax

```ebnf
<set-password> ::= 'SET PASSWORD' [ 'FOR' <account> ]
                   ( '=' <string-literal> | 'TO RANDOM' )
                   [ 'REPLACE' <string-literal> ]
                   [ 'RETAIN CURRENT PASSWORD' ]
```

## Behavior and Limitations

- Without FOR, the password of CURRENT_USER is changed. REPLACE verifies the old password when the account requires the current password.
- RETAIN CURRENT PASSWORD keeps the old hash as a secondary credential, enabling uninterrupted rotation.
- TO RANDOM generates a credential securely and returns it via the result; insufficient privileges or an unmet policy returns a clear error. CREATE/ALTER USER use the different spelling IDENTIFIED BY RANDOM PASSWORD.

## Privileges and Result Details

Self-service changes need no CREATE USER grant, but changing another account requires it. Retaining a secondary password checks APPLICATION_PASSWORD_ADMIN, and protected system accounts require SYSTEM_USER authority. Password policy/current-password requirements still apply. SET PASSWORD is an implicit-commit account operation.

TO RANDOM returns user, host, generated password and auth_factor as described in [CREATE USER](create-user.md#privileges-and-result-details). It does not reveal an old password. Existing API keys are not deleted by password rotation. Use ALTER USER ... DISCARD OLD PASSWORD to finish dual-password rotation.

## Examples

```sql
SET PASSWORD = 'new-secret' REPLACE 'old-secret';
SET PASSWORD FOR 'app'@'10.%' TO RANDOM
RETAIN CURRENT PASSWORD;
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)

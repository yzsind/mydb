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

## Examples

```sql
SET PASSWORD = 'new-secret' REPLACE 'old-secret';
SET PASSWORD FOR 'app'@'10.%' TO RANDOM
RETAIN CURRENT PASSWORD;
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)

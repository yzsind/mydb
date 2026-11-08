# `SHOW CREATE USER`

## Description

Returns the CREATE USER text that can rebuild an account's authentication and policy attributes.

## Syntax

```ebnf
<show-create-user> ::= 'SHOW CREATE USER'
                       ( <account> | 'CURRENT_USER' [ '()' ] )
```

## Behavior and Limitations

- Sensitive credentials are shown as the plugin-format authentication string; the plaintext password is never returned.
- Viewing another account requires SELECT on the mysql schema; a nonexistent account returns an error. Viewing your own matched account does not need that additional grant.

## Privileges and Result Details

Returns one VARCHAR column named `CREATE USER for <user>@<host>` for the exact account. It includes plugin-format authentication material, never the plaintext password. Access to another account needs SELECT on mysql; protect the result as credential metadata. An absent account is an account-operation error, not an empty success. Role/default-role grants are inspected separately with SHOW GRANTS. Reading this definition does not commit or change the account.

## Examples

```sql
SHOW CREATE USER CURRENT_USER();
SHOW CREATE USER 'app'@'10.%';
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)

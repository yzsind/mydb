# `SET DEFAULT ROLE`

## Description

Configures the roles activated by default after an account logs in.

## Syntax

```ebnf
<set-default-role> ::= 'SET DEFAULT ROLE'
                       ( 'NONE' | 'ALL' | <role-list> )
                       'TO' <account> { ',' <account> }
```

## Behavior and Limitations

- The selected roles must already be granted to the target account.
- The change affects subsequent logins; the current session requires a separate SET ROLE.
- Multi-target changes are committed atomically.

## Privileges and Result Details

Setting defaults for yourself needs no CREATE USER privilege, but selected roles must already be applicable. Setting another account's defaults requires CREATE USER, with SYSTEM_USER protection as applicable. This account catalog statement implicitly commits preceding DML. ALL records the current granted role set, not a promise to auto-add every future role grant; NONE removes stored defaults. The current connection uses SET ROLE to change its active set.

## Examples

```sql
SET DEFAULT ROLE 'reader' TO 'report'@'%';
SET DEFAULT ROLE ALL TO 'app'@'10.%';
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)

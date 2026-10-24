# `ALTER USER`

## Description

Modifies the authentication, password policy, SSL, resource, lock status, or default authentication attributes of one or more accounts.

## Syntax

```ebnf
<alter-user> ::= 'ALTER USER' [ 'IF EXISTS' ]
                 <user-change> { ',' <user-change> }
                 [ <tls-option> ] [ <resource-options> ]
                 [ <password-options> ]
<user-change> ::= <account>
                  [ 'IDENTIFIED' <authentication-clause> ]
                  [ 'ACCOUNT' ( 'LOCK' | 'UNLOCK' ) ]
```

## Behavior and Limitations

- Multi-account changes are validated first and then committed atomically; IF EXISTS produces a 3162 Note for missing accounts.
- Supports the current account-model fields such as PASSWORD REQUIRE CURRENT, FAILED_LOGIN_ATTEMPTS, PASSWORD_LOCK_TIME, and RETAIN CURRENT PASSWORD.
- Changing authentication safely updates the related caches and account indexes; whether the current connection remains valid follows credential rotation semantics.

## Examples

```sql
ALTER USER 'app'@'10.%' IDENTIFIED BY 'new-secret'
RETAIN CURRENT PASSWORD;

ALTER USER IF EXISTS 'report'@'%' ACCOUNT UNLOCK;
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)

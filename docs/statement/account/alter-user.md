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

## Privileges and Result Details

Global CREATE USER is required for changing other accounts or account policy. A self-service password change can proceed without it, but accompanying plugin/TLS/quota/lock/policy changes are still privileged. RETAIN/DISCARD secondary passwords additionally check APPLICATION_PASSWORD_ADMIN. SYSTEM_USER protects system accounts. This statement implicitly commits previous DML and atomically applies a multi-account change.

CURRENT_USER() selects the matched account. REPLACE supplies the old plaintext for PASSWORD REQUIRE CURRENT validation. RETAIN CURRENT PASSWORD preserves the previous hash; DISCARD OLD PASSWORD removes it. Password-policy DEFAULT clears a per-account override; omission leaves it unchanged. ACCOUNT UNLOCK clears failed-login locking; rotating a password alone does not promise to clear that temporary lock. API keys are independent credentials.

```sql
ALTER USER CURRENT_USER() IDENTIFIED BY 'new-secret' REPLACE 'old-secret';
ALTER USER 'app'@'10.%' DISCARD OLD PASSWORD;
```

## Examples

```sql
ALTER USER 'app'@'10.%' IDENTIFIED BY 'new-secret'
RETAIN CURRENT PASSWORD;

ALTER USER IF EXISTS 'report'@'%' ACCOUNT UNLOCK;
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)

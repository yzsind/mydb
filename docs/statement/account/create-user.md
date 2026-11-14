# `CREATE USER`

## Description

Atomically creates one or more accounts, configuring the authentication plugin, password policy, SSL, resource limits, lock status, and default roles.

## Syntax

```ebnf
<create-user> ::= 'CREATE USER' [ 'IF NOT EXISTS' ]
                  <user-spec> { ',' <user-spec> }
                  [ 'DEFAULT ROLE' <role-list> ]
                  [ <tls-option> ] [ <resource-options> ]
                  [ <password-options> ] [ 'ACCOUNT' ( 'LOCK' | 'UNLOCK' ) ]
<user-spec> ::= <account>
                [ 'IDENTIFIED' <authentication-clause> ]
<authentication-clause> ::= 'BY' <string-literal>
                             | 'BY RANDOM PASSWORD'
                             | 'WITH' <identifier>
                               [ ( 'BY' <string-literal> | 'BY RANDOM PASSWORD'
                                 | 'AS' <string-literal> ) ]
```

## Behavior and Limitations

- Supports caching_sha2_password, mysql_native_password, and sha256_password; AS accepts the plugin-format authentication string, while BY accepts plaintext and hashes it.
- Multiple accounts are fully validated first and then committed atomically; target conflicts never overwrite existing accounts. IF NOT EXISTS produces a 3163 Note.
- Options such as SSL, MAX_QUERIES/UPDATES/CONNECTIONS, MAX_USER_CONNECTIONS, and password history/expiration/locking apply to all accounts in the statement.
- BY RANDOM PASSWORD returns a result row containing the generated credential; it is not a SHOW WARNINGS message. Treat the result as a secret.

## Account Options

```ebnf
<tls-option> ::= 'REQUIRE' ( 'NONE' | 'SSL' | 'X509'
                 | <tls-attribute> { [ 'AND' ] <tls-attribute> } )
<tls-attribute> ::= ( 'CIPHER' | 'ISSUER' | 'SUBJECT' ) <string-literal>
<resource-options> ::= 'WITH' <resource-option> { <resource-option> }
<resource-option> ::= ( 'MAX_QUERIES_PER_HOUR' | 'MAX_UPDATES_PER_HOUR'
                     | 'MAX_CONNECTIONS_PER_HOUR' | 'MAX_USER_CONNECTIONS' )
                     <unsigned-integer>
<password-options> ::= <password-option> { <password-option> }
<password-option> ::= 'PASSWORD EXPIRE' [ 'DEFAULT' | 'NEVER'
                       | 'INTERVAL' <unsigned-integer> 'DAY' ]
                    | 'PASSWORD HISTORY' ( 'DEFAULT' | <unsigned-integer> )
                    | 'PASSWORD REUSE INTERVAL'
                      ( 'DEFAULT' | <unsigned-integer> 'DAY' )
                    | 'PASSWORD REQUIRE CURRENT' [ 'DEFAULT' | 'OPTIONAL' ]
                    | 'FAILED_LOGIN_ATTEMPTS' <unsigned-integer>
                    | 'PASSWORD_LOCK_TIME' ( 'UNBOUNDED' | <unsigned-integer> )
```

| Setting | Meaning/default |
| --- | --- |
| REQUIRE NONE/SSL/X509 or attributes | No account-specific TLS requirement / encrypted transport / verified client certificate / matching certificate attributes |
| Per-hour query/update/connection quotas | Nonnegative account limits; zero removes the per-account hourly cap |
| MAX_USER_CONNECTIONS | Concurrent account connections; zero uses the server's global rule where applicable |
| ACCOUNT LOCK/UNLOCK | Explicit login lock, separate from temporary failed-login lockout |
| PASSWORD EXPIRE/HISTORY/REUSE/REQUIRE CURRENT DEFAULT | Inherit the corresponding global policy; an explicit account override takes precedence |
| FAILED_LOGIN_ATTEMPTS and PASSWORD_LOCK_TIME | Temporary failure threshold and lock duration in days; explicit ACCOUNT UNLOCK clears the failed-login lock |
| COMMENT / ATTRIBUTE | Text comment or validated JSON account metadata; ALTER applies metadata patch semantics |
| DEFAULT ROLE | Roles to grant/activate by default, checked under role-delegation authority |

These are real account-level quotas/policies and are separate from unsupported RESOURCE GROUP/VCPU scheduling. Policy numeric ranges are validated by their target field; unknown or unsupported options are errors. Plaintext password changes are checked by validate_password when that component is installed. AS authentication strings are validated in the selected plugin's format rather than re-hashed as plaintext.

## Privileges and Result Details

Requires global CREATE USER; system-account/definer dependencies add their own checks. The statement implicitly commits preceding user DML, then validates/commits its account list atomically. Omitted host is `%`; use explicit hosts for reproducible account identities. Omitted authentication plugin uses the configured default.

RANDOM PASSWORD returns `user`, `host`, `generated password` (VARCHAR) and `auth_factor` (TINYINT, currently 1). A normal create returns OK. Unknown plugins, malformed plugin hashes and password-policy failures are errors; IF NOT EXISTS skips duplicates with Note 3163 rather than changing their policy.

## Examples

```sql
CREATE USER 'app'@'10.%' IDENTIFIED BY 'strong-secret'
REQUIRE SSL
WITH MAX_USER_CONNECTIONS 20;

CREATE USER IF NOT EXISTS 'report'@'%'
IDENTIFIED WITH caching_sha2_password BY RANDOM PASSWORD
ACCOUNT LOCK;
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)

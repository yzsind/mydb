# `REVOKE`

## Description

Revokes direct object/administrative privileges, GRANT OPTION or roles from the named accounts.

## Syntax

```ebnf
<revoke> ::= 'REVOKE'
             ( <privilege-list> 'ON' <grant-scope>
             | <role-list> )
             'FROM' <account> { ',' <account> }
             | 'REVOKE ALL PRIVILEGES, GRANT OPTION FROM'
               <account> { ',' <account> }
```

## Behavior and Limitations

- Only the specified direct grants are revoked; privileges obtained through other roles are neither copied nor rewritten.
- Role revocation updates the closure; the same role can still be inherited through other paths.
- Targets, privileges, and grantor eligibility are fully validated before commit.

## Privileges and Result Details

Authorization and scope validation match the corresponding grant family; SYSTEM_USER protection still applies. REVOKE implicitly commits preceding DML. `REVOKE GRANT OPTION FOR ... ON ... FROM ...` removes delegation without necessarily removing use privilege. REVOKE ALL PRIVILEGES, GRANT OPTION removes direct grants from named accounts, not their owned business tables or every possible inherited role grant.

With partial_revokes enabled, a database-scope REVOKE can record a restriction against an inherited global grant; it is not just deletion of a direct row. GRANT PROXY is unsupported (1235); REVOKE PROXY can clean up stored proxy grants. AI model use has a separate [model scope](grant.md#privileges-and-scope).

## Examples

```sql
REVOKE INSERT ON app.* FROM 'writer'@'%';
REVOKE 'reader' FROM 'report'@'%';
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)

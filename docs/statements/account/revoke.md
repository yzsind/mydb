# `REVOKE`

## Description

Revokes object privileges, GRANT OPTION, roles, or all privileges from the current user.

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

## Examples

```sql
REVOKE INSERT ON app.* FROM 'writer'@'%';
REVOKE 'reader' FROM 'report'@'%';
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)

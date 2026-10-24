# `GRANT`

## Description

Grants object or administrative privileges, or grants roles to accounts.

## Syntax

```ebnf
<grant> ::= 'GRANT'
            ( <privilege> [ '(' <identifier-list> ')' ]
              { ',' <privilege> [ '(' <identifier-list> ')' ] }
              'ON' <grant-scope>
            | <role-list> )
            'TO' <account> { ',' <account> }
            [ 'WITH GRANT OPTION' | 'WITH ADMIN OPTION' ]
<grant-scope> ::= '*.*' | <identifier> '.*'
                  | <qualified-name>
                  | ( 'PROCEDURE' | 'FUNCTION' ) <qualified-name>
```

## Behavior and Limitations

- Privilege names, object types, and grantor privileges are strictly validated; unknown privileges are not recorded as strings.
- Role grants maintain the closure and prevent cycles; WITH ADMIN OPTION controls further granting of roles.
- Object grants and role edges are written atomically to the data dictionary; current and new sessions resolve them according to role activation rules.

## Examples

```sql
GRANT SELECT, INSERT ON app.* TO 'writer'@'%';
GRANT 'reader' TO 'report'@'%' WITH ADMIN OPTION;
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)

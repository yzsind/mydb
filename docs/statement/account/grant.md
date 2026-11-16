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

## Privileges and Scope

| Scope | Examples and boundaries |
| --- | --- |
| Global `*.*` | Static server privileges and registered dynamic privileges such as BACKUP_ADMIN, HTTP_ADMIN, AI_ADMIN; unknown dynamic names are rejected |
| Database `db.*` | Database/object privileges; dynamic administration privileges are not legal here |
| Table `db.table` | Table privileges; `(col1,col2)` restricts legal column privileges such as SELECT/INSERT/UPDATE/REFERENCES |
| PROCEDURE/FUNCTION `db.name` | Routine EXECUTE/ALTER ROUTINE/appropriate delegation |
| AI MODEL `db.name` | GRANT/REVOKE AI_USE on one existing stable model identity; see [AI administration](../../ai.md#administration-and-visibility) |
| Roles without ON | Delegates existing role accounts with optional WITH ADMIN OPTION |

Model grants use `GRANT AI_USE ON AI MODEL db.model TO account`; model recreation does not revive old model-identity grants. Global AI_USE explicitly includes future models. AI catalog/grant mutations are rejected while outgoing MySQL binlogging is enabled. Ordinary ALL PRIVILEGES is scope-dependent; granting ALL on one database does not include global SUPER or an RLS bypass.

```sql
GRANT AI_USE ON AI MODEL statement_examples.local_chat TO 'app'@'%';
REVOKE AI_USE ON AI MODEL statement_examples.local_chat FROM 'app'@'%';
```

## Privileges and Result Details

Privilege grants require authority for each named privilege with GRANT OPTION at the applicable scope. Role grants require ROLE_ADMIN or ADMIN OPTION for that role (otherwise 3523). Granting a role must not create a cycle. SYSTEM_USER protection applies to protected targets. GRANT implicitly commits preceding DML before its atomic catalog mutation.

WITH GRANT OPTION belongs to privilege delegation; WITH ADMIN OPTION belongs to role delegation. Neither turns an unknown privilege into a valid grant. AS account WITH ROLE controls the grant context where supported. A grant must be activated through role rules before becoming a role-derived privilege in this session.

## Examples

```sql
GRANT SELECT, INSERT ON app.* TO 'writer'@'%';
GRANT 'reader' TO 'report'@'%' WITH ADMIN OPTION;
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)

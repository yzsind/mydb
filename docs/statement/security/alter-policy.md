# `ALTER POLICY`

## Description

Manages or inspects row-security policy metadata.

## Syntax

```ebnf
<alter-policy> ::= 'ALTER POLICY' <identifier> 'ON' <qualified-name>
                   ( 'RENAME TO' <identifier>
                   | [ 'TO' ( 'PUBLIC' | <role-list> ) ]
                     [ 'USING' '(' <expr> ')' ]
                     [ 'WITH CHECK' '(' <expr> ')' ] )
```

## Behavior, Privileges, and Results

Requires global `ROW_SECURITY_ADMIN`. The target must be a persistent user base table, not a view, system table, or temporary table. Policy DDL uses an implicit-commit boundary and atomic dictionary publication. MySQL receivers do not implement these extensions; use native replication for equivalent enforcement. See [policy dependencies and expression restrictions](../../row-level-security.md#policies).

At least one change is required. Rename cannot be combined with roles or predicates. Omitted attributes remain unchanged; changed clauses capture their new expression settings. The command cannot change PERMISSIVE/RESTRICTIVE or the command category. Returns OK.

## Example

```sql
ALTER POLICY tenant_access ON accounts RENAME TO account_access;
```

## See Also

- [Row-level security](../../row-level-security.md)
- [Security index](README.md)

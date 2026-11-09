# `CREATE POLICY`

## Description

Manages or inspects row-security policy metadata.

## Syntax

```ebnf
<create-policy> ::= 'CREATE POLICY' <identifier> 'ON' <qualified-name>
                    [ 'AS' ( 'PERMISSIVE' | 'RESTRICTIVE' ) ]
                    [ 'FOR' ( 'ALL' | 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE' ) ]
                    [ 'TO' ( 'PUBLIC' | <role-list> ) ]
                    [ 'USING' '(' <expr> ')' ]
                    [ 'WITH CHECK' '(' <expr> ')' ]
```

## Behavior, Privileges, and Results

Requires global `ROW_SECURITY_ADMIN`. The target must be a persistent user base table, not a view, system table, or temporary table. Policy DDL uses an implicit-commit boundary and atomic dictionary publication. MySQL receivers do not implement these extensions; use native replication for equivalent enforcement. See [policy dependencies and expression restrictions](../../row-level-security.md#policies).

Defaults are PERMISSIVE, ALL, PUBLIC. Applicable permissive rules combine with OR; restrictive rules combine with AND, and at least one permissive rule must allow the row. USING filters old rows; WITH CHECK validates new coerced/generated rows. NULL does not authorize a row. INSERT cannot specify USING; SELECT/DELETE cannot specify WITH CHECK. For ALL/UPDATE, an omitted check inherits USING. Returns OK.

## Example

```sql
CREATE POLICY tenant_access ON accounts FOR ALL TO PUBLIC
USING (tenant_id=CAST(RLS_CONTEXT('tenant_id') AS UNSIGNED))
WITH CHECK (tenant_id=CAST(RLS_CONTEXT('tenant_id') AS UNSIGNED));
```

## See Also

- [Row-level security](../../row-level-security.md)
- [Security index](README.md)

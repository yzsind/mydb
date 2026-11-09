# `SET [LOCAL] RLS CONTEXT`

## Description

Changes trusted row-security context for the current session.

## Syntax

```ebnf
<set-rls-context> ::= 'SET' [ 'LOCAL' ] 'RLS CONTEXT' <string-literal>
                      '=' ( <literal> | '?' )
```

## Behavior, Privileges, and Results

Requires global `RLS_CONTEXT_ADMIN` and a top-level statement; stored routines and triggers cannot change trusted context. Returns OK. Values become nullable text; missing keys read as NULL through RLS_CONTEXT(). Expressions other than a literal or prepared parameter are rejected. LOCAL requires an explicit transaction and is restored at transaction completion or savepoint rollback. Without LOCAL the setting is session-scoped. The application must authenticate and validate the tenant before assigning trusted context.

## Example

```sql
START TRANSACTION;
SET LOCAL RLS CONTEXT 'tenant_id'='42';
SELECT RLS_CONTEXT('tenant_id');
ROLLBACK;
```

## See Also

- [Connection-pool context](../../row-level-security.md#connection-pools)
- [Security index](README.md)

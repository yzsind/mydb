# `ALTER TABLE ENABLE / DISABLE ROW LEVEL SECURITY`

## Description

Manages or inspects row-security policy metadata.

## Syntax

```ebnf
<alter-row-security> ::= 'ALTER TABLE' <qualified-name>
                         ( 'ENABLE' | 'DISABLE' ) 'ROW LEVEL SECURITY'
```

## Behavior, Privileges, and Results

Requires global `ROW_SECURITY_ADMIN`. The target must be a persistent user base table, not a view, system table, or temporary table. Policy DDL uses an implicit-commit boundary and atomic dictionary publication. MySQL receivers do not implement these extensions; use native replication for equivalent enforcement. See [policy dependencies and expression restrictions](../../row-level-security.md#policies).

ENABLE activates existing policies; no applicable policy means deny. DISABLE retains definitions but stops enforcement. Table ownership is not an exemption. FORCE/NO FORCE ROW LEVEL SECURITY is rejected. Returns OK.

## Example

```sql
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
```

## See Also

- [Row-level security](../../row-level-security.md)
- [Security index](README.md)

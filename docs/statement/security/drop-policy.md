# `DROP POLICY`

## Description

Manages or inspects row-security policy metadata.

## Syntax

```ebnf
<drop-policy> ::= 'DROP POLICY' [ 'IF EXISTS' ] <identifier>
                  'ON' <qualified-name>
```

## Behavior, Privileges, and Results

Requires global `ROW_SECURITY_ADMIN`. The target must be a persistent user base table, not a view, system table, or temporary table. Policy DDL uses an implicit-commit boundary and atomic dictionary publication. MySQL receivers do not implement these extensions; use native replication for equivalent enforcement. See [policy dependencies and expression restrictions](../../row-level-security.md#policies).

Removing the last applicable permissive policy from an enabled table denies affected rows; it does not disable row security. Returns OK on successful removal.

## Example

```sql
DROP POLICY IF EXISTS account_access ON accounts;
```

## See Also

- [Row-level security](../../row-level-security.md)
- [Security index](README.md)

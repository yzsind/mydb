# `SHOW CREATE POLICY`

## Description

Manages or inspects row-security policy metadata.

## Syntax

```ebnf
<show-create-policy> ::= 'SHOW CREATE POLICY' <identifier>
                         'ON' <qualified-name>
```

## Behavior, Privileges, and Results

Requires global `ROW_SECURITY_ADMIN` on a persistent user base table.

Returns string columns `Policy` and `Create Policy`, containing the name and canonical policy SQL. It does not commit a transaction or modify enforcement. A missing policy returns an error.

## Example

```sql
SHOW CREATE POLICY tenant_access ON accounts;
```

## See Also

- [Row-level security](../../row-level-security.md)
- [Security index](README.md)

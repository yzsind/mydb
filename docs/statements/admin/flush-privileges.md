# `FLUSH PRIVILEGES`

## Description

Refreshes privilege-related runtime state.

## Syntax

```ebnf
<flush-privileges> ::= 'FLUSH PRIVILEGES'
```

## Behavior, Privileges, and Limitations

- The authoritative source of accounts and grants is transactionally written to the data dictionary; this command exists for compatibility with administrative procedures and refreshes the related caches.
- Requires the RELOAD privilege or an equivalent administrative privilege; it does not commit user DML transactions.

## Examples

```sql
FLUSH PRIVILEGES;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)

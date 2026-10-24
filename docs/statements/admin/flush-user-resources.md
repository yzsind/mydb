# `FLUSH USER_RESOURCES`

## Description

Resets the resource quota counters of all accounts.

## Syntax

```ebnf
<flush-user-resources> ::= 'FLUSH'
                          ( 'USER_RESOURCES' | 'USER RESOURCES' )
```

## Behavior, Privileges, and Limitations

- Zeroes per-account runtime counters such as queries/updates/connections; account limit values are not modified.
- Requires administrative privileges; currently active connections are not disconnected by it.

## Examples

```sql
FLUSH USER_RESOURCES;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)

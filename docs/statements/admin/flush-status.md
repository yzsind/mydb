# `FLUSH STATUS`

## Description

Resets process/session status counters that have a well-defined zero point.

## Syntax

```ebnf
<flush-status> ::= 'FLUSH STATUS'
```

## Behavior, Privileges, and Limitations

- Only peak/interval counters that the implementation allows to be cleared are reset; running totals and most storage-level cumulative counters remain unchanged.
- The result can be verified by reading SHOW STATUS; the RELOAD privilege is required.

## Examples

```sql
FLUSH STATUS;
SHOW GLOBAL STATUS LIKE 'Max_used_connections';
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)

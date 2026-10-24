# `SHOW REPLICAS`

## Description

Lists replicas connected to this instance's binlog source; SHOW SLAVE HOSTS is a deprecated alias.

## Syntax

```ebnf
<show-replicas> ::= 'SHOW REPLICAS' | 'SHOW SLAVE HOSTS'
```

## Behavior, Prerequisites, and Limitations

- Shows only replicas observable through registration/connection; it does not represent all offline replica configurations.
- The alias keeps the MySQL-compatible column names and produces a deprecation diagnostic.

## Examples

```sql
SHOW REPLICAS;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

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

## Privileges, Results, and Operational Details

Requires REPLICATION SLAVE. Columns are Server_id, Host, Port, Source_id, Replica_UUID. The UUID can be empty when a registered consumer did not advertise it. These rows come from logical registration, not native WAL retention slots; offline configurations and unregistered readers are not a topology inventory. Disabled logging returns an explicit configuration error.

## Examples

```sql
SHOW REPLICAS;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

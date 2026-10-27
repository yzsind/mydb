# `SHOW REPLICA STATUS`

## Description

Shows the thread, source, position, GTID, filter, and error state of a MySQL→mydb channel; SHOW SLAVE STATUS is a deprecated alias.

## Syntax

```ebnf
<show-replica-status> ::= ( 'SHOW REPLICA STATUS' | 'SHOW SLAVE STATUS' )
                          [ 'FOR CHANNEL' <string-literal> ]
```

## Behavior, Prerequisites, and Limitations

- When the manager is not initialized, a canonical empty result set is returned, matching a fresh-server probe.
- The legacy spelling uses Slave_*/Master_* column names and produces a deprecation warning.
- The replica receiver is included in every build.

## Examples

```sql
SHOW REPLICA STATUS FOR CHANNEL 'main';
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

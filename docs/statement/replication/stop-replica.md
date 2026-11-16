# `STOP REPLICA`

## Description

Stops the IO/SQL threads of a MySQL→mydb channel; STOP SLAVE is a deprecated alias.

## Syntax

```ebnf
<stop-replica> ::= ( 'STOP REPLICA' | 'STOP SLAVE' )
                   [ <thread-selector> ]
                   [ 'FOR CHANNEL' <string-literal> ]
```

## Behavior, Prerequisites, and Limitations

- Omitting the selector stops both threads; stopping an already-stopped channel is a safe state transition.
- Stopping does not delete the relay log or the channel configuration.

## Privileges, Results, and Operational Details

Requires REPLICATION_SLAVE_ADMIN. Omitted FOR CHANNEL selects `default`. The thread-selector syntax is shared with START and allows IO_THREAD/SQL_THREAD in either order. Stopping IO can leave SQL consuming retained relay work; stopping SQL can leave IO receiving more relay data. Progress/configuration/relay history is retained. Legacy STOP SLAVE emits warning 1287. A timeout/error must be resolved before assuming channel quiescence.

Stopping threads does not by itself promise that `mydb_replica_read_only` becomes
OFF. For a planned migration cutover, confirm every relevant thread is stopped
and inspect the global guard before enabling local writes. See the
[migration workflow](../../mysql-migration.md#validate-and-switch).

## Examples

```sql
STOP REPLICA SQL_THREAD FOR CHANNEL 'main';
STOP REPLICA FOR CHANNEL 'main';
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

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

## Examples

```sql
STOP REPLICA SQL_THREAD FOR CHANNEL 'main';
STOP REPLICA FOR CHANNEL 'main';
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

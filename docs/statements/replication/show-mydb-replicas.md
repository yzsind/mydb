# `SHOW MYDB REPLICAS`

## Description

Lists registered replica slots with their connection and retention state on the native master.

## Syntax

```ebnf
<show-mydb-replicas> ::= 'SHOW MYDB REPLICAS'
```

## Behavior, Prerequisites, and Limitations

- Returns Replica_UUID, Connected, Retention_State, Replica_Acked_LSN, Lag_LSN, and more.
- Offline slots remain visible until DROP MYDB REPLICA.

## Examples

```sql
SHOW MYDB REPLICAS;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

`Replica_Acked_LSN` is the slot ack/retention watermark maintained by the master; at initialization it may be taken from a backup or a resume boundary, and it does not represent a direct read of the replica's durable-applied state.

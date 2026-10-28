# `SHOW MYDB MASTER STATUS`

## Description

Shows the cluster, timeline, LSN, and generation of a mydb native physical replication master.

## Syntax

```ebnf
<show-mydb-master-status> ::= 'SHOW MYDB MASTER STATUS'
```

## Behavior, Prerequisites, and Limitations

- Belongs only to the mydb→mydb protocol v3 and is not interchangeable with SHOW BINARY LOG STATUS.
- `Active_Slot_Count`, `Invalidated_Slot_Count`, and `Abandoned_Slot_Count` each summarize retention state; `Min_Slot_Acked_LSN` counts only active slots; for connection state, see SHOW MYDB REPLICAS.
- When the node is not a master or the manager is not initialized, an explicit role/configuration status is returned.

## Examples

```sql
SHOW MYDB MASTER STATUS;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

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
- With an initialized manager, the row includes the actual Role and listener state even when the node is not currently master. Without a native manager, execution returns 1235.

## Privileges, Results, and Operational Details

Requires SYSTEM_VARIABLES_ADMIN. [All ordered fields/types](status-fields.md#show-mydb-master-status) distinguish current/durable/available WAL, active retention floor, listener state and semi-sync outcomes. Min_Slot_Acked_LSN is NULL without active slots. Writes_Frozen can reflect a capture barrier as well as a role fence. A slot ack is not a replica SQL read-view guarantee.

## Examples

```sql
SHOW MYDB MASTER STATUS;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

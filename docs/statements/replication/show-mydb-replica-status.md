# `SHOW MYDB REPLICA STATUS`

## Description

Shows the source, receive, apply, and durable LSN state of this node as a mydb native replica.

## Syntax

```ebnf
<show-mydb-replica-status> ::= 'SHOW MYDB REPLICA STATUS'
```

## Behavior, Prerequisites, and Limitations

- LSN, epoch, timeline, resync, and transport state come from the persisted progress/receiver.
- Read-only query; it does not change the receiver.

## Privileges, Results, and Operational Details

Requires SYSTEM_VARIABLES_ADMIN. [All ordered fields/types](status-fields.md#show-mydb-replica-status) distinguish receive, durable, applied and durable-applied boundaries. Receive_Apply_Gap_LSN is LSN distance, not seconds. Last_Update_Ms is Unix milliseconds; Last_Status/Resync_Required can be NULL. The role row is diagnostic even while stopped; inspect Role and Receiver_Running before inferring a running replica.

## Examples

```sql
SHOW MYDB REPLICA STATUS;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

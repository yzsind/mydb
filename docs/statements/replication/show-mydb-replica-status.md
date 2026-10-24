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

## Examples

```sql
SHOW MYDB REPLICA STATUS;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

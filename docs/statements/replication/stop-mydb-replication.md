# `STOP MYDB REPLICATION`

## Description

Stops the native replica receiver.

## Syntax

```ebnf
<stop-mydb-replication> ::= 'STOP MYDB REPLICATION'
```

## Behavior, Prerequisites, and Limitations

- Stopping preserves progress and the master endpoint, so replication can be started again later.
- It waits a bounded time for the receiver to wind down and persist its progress.

## Examples

```sql
STOP MYDB REPLICATION;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

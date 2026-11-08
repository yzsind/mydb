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

## Privileges, Results, and Operational Details

Requires SYSTEM_VARIABLES_ADMIN. The SQL stop waits up to 10 seconds for the receiver to exit; timeout returns an explicit stopping error and prevents a role transition/new overlapping START until the old apply work exits. It retains progress/endpoint and does not make a physical replica writable. Inspect Receiver_Running and Last_Status after a timeout rather than interpreting request delivery as quiescence.

## Examples

```sql
STOP MYDB REPLICATION;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

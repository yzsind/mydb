# `DROP MYDB REPLICA`

## Description

Drops the retention slot of the specified replica UUID on the native master.

## Syntax

```ebnf
<drop-mydb-replica> ::= 'DROP MYDB REPLICA' <string-literal>
```

## Behavior, Prerequisites, and Limitations

- A connected replica cannot be dropped; it must be isolated/stopped first.
- Dropping removes the slot's protection of WAL retention; an unknown UUID returns an explicit error.

## Privileges, Results, and Operational Details

Requires SYSTEM_VARIABLES_ADMIN on the native master. Stop/isolate a connected receiver first; do not confuse a disconnected Active slot with an Invalidated/Abandoned slot. Deletion removes the retention identity and does not wait for a stale replica to receive WAL. Rejoining after required history is reclaimed needs a supported seed/resume boundary; it cannot revive deleted WAL by reusing a UUID.

## Examples

```sql
DROP MYDB REPLICA '550e8400-e29b-41d4-a716-446655440000';
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

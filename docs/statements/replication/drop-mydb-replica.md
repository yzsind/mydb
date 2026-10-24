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

## Examples

```sql
DROP MYDB REPLICA '550e8400-e29b-41d4-a716-446655440000';
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

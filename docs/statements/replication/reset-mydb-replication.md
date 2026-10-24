# `RESET MYDB REPLICATION`

## Description

Clears native replication progress, source configuration, and resync markers, restoring a non-master node to standalone.

## Syntax

```ebnf
<reset-mydb-replication> ::= 'RESET MYDB REPLICATION'
```

## Behavior, Prerequisites, and Limitations

- Not allowed on a node in the master role; the command stops the receiver first.
- High-impact operation: to serve as a replica again later, reconfiguration or a base backup is usually required.

## Examples

```sql
STOP MYDB REPLICATION;
RESET MYDB REPLICATION;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

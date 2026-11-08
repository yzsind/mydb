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

## Privileges, Results, and Operational Details

Requires SYSTEM_VARIABLES_ADMIN and is rejected on the master role. The receiver is stopped before clearing progress/source/resync state; underlying business data is retained. It also clears the engine physical-replica flag, restoring standalone write eligibility. Reconfiguring the node as a replica again requires reseeding when source/role history cannot safely continue. This does not delete the old master's retention slot; manage that slot separately.

## Examples

```sql
STOP MYDB REPLICATION;
RESET MYDB REPLICATION;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

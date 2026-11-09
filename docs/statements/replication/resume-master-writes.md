# `RESUME MASTER WRITES`

## Description

Cancels the native master write fence and resumes writes.

## Syntax

```ebnf
<resume-master-writes> ::= 'RESUME MASTER WRITES'
```

## Behavior, Prerequisites, and Limitations

- Allowed only on a master; clears the fence and advances the generation.
- Intended for aborting a switchover; it must not be used to resume writes on the old master after a new master has been promoted.

## Privileges, Results, and Operational Details

Requires SYSTEM_VARIABLES_ADMIN on the native master. This is an explicit removal of this node's persisted write fence and advances role state. It is suitable when aborting a planned switch before another master has taken ownership. It does not fence another node or roll back a completed promotion; confirm that the old node still owns the writer role before issuing it.

## Examples

```sql
RESUME MASTER WRITES;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

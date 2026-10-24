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

## Examples

```sql
RESUME MASTER WRITES;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

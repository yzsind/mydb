# `SHOW BINARY LOGS`

## Description

Lists available binlog files with their sizes; SHOW MASTER LOGS is an alias.

## Syntax

```ebnf
<show-binary-logs> ::= 'SHOW BINARY LOGS' | 'SHOW MASTER LOGS'
```

## Behavior, Prerequisites, and Limitations

- Results come from the binlog manager's file catalog; when binlog is not enabled, the result is empty or an explicit configuration status is returned.
- Requires view privileges such as REPLICATION CLIENT.

## Examples

```sql
SHOW BINARY LOGS;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

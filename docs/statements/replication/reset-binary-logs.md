# `RESET BINARY LOGS AND GTIDS`

## Description

Resets binlog files and GTID execution state; RESET MASTER is a deprecated alias.

## Syntax

```ebnf
<reset-binlogs> ::= 'RESET BINARY LOGS AND GTIDS' | 'RESET MASTER'
```

## Behavior, Prerequisites, and Limitations

- High-impact operation: deletes/rebuilds the source's binlog history and resets GTID state; existing replicas may have to be reconfigured.
- Executes under the commit gate and requires administrative privileges; the deprecated alias produces a warning.

## Examples

```sql
RESET BINARY LOGS AND GTIDS;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

# `SHOW BINARY LOG STATUS`

## Description

Shows the current MySQL row binlog file, position, and GTID state; SHOW MASTER STATUS is a deprecated alias.

## Syntax

```ebnf
<show-binlog-status> ::= 'SHOW BINARY LOG STATUS'
                         | 'SHOW MASTER STATUS'
```

## Behavior, Prerequisites, and Limitations

- The default build can already serve as a binlog source; an active file status exists only after binlog is enabled/initialized.
- Returns compatibility columns such as File, Position, Binlog_Do_DB, Binlog_Ignore_DB, and Executed_Gtid_Set.

## Examples

```sql
SHOW BINARY LOG STATUS;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

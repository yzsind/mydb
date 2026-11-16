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

## Privileges, Results, and Operational Details

Requires REPLICATION CLIENT. All five columns and types are in [status fields](status-fields.md#binlog-and-relay-event-listings). Disabled logging returns 1235 rather than a guaranteed empty status. File/position and Executed_Gtid_Set describe logical source history, not a native WAL backup receipt. The deprecated alias emits a diagnostic.

## Examples

```sql
SHOW BINARY LOG STATUS;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

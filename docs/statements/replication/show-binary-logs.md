# `SHOW BINARY LOGS`

## Description

Lists available binlog files with their sizes; SHOW MASTER LOGS is an alias.

## Syntax

```ebnf
<show-binary-logs> ::= 'SHOW BINARY LOGS' | 'SHOW MASTER LOGS'
```

## Behavior, Prerequisites, and Limitations

- Results come from the binlog manager's file catalog; when binary logging is disabled, execution returns 1235, `binary logging is disabled`.
- Requires view privileges such as REPLICATION CLIENT.

## Privileges, Results, and Operational Details

Requires REPLICATION CLIENT. Log_name is a source filename, File_size is bytes, Encrypted is currently No. An empty live file set is different from disabled logging (1235). This listing is not proof a replica has consumed a file or that PURGE will preserve its position.

## Examples

```sql
SHOW BINARY LOGS;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

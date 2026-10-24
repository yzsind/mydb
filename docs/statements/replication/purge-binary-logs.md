# `PURGE BINARY LOGS`

## Description

Purges old binlog files by target file or time; MASTER LOGS is a deprecated alias.

## Syntax

```ebnf
<purge-binlogs> ::= 'PURGE' ( 'BINARY' | 'MASTER' ) 'LOGS'
                     ( 'TO' <string-literal> | 'BEFORE' <string-literal> )
```

## Behavior, Prerequisites, and Limitations

- TO deletes logs before the named existing file and retains that file. BEFORE accepts `YYYY-MM-DD HH:MM:SS` or `YYYY-MM-DDTHH:MM:SS`, interpreted in the server host's local time zone, and selects by file modification time. The active file is retained.
- Purging does not wait for all downstream replicas or preserve files according to their read positions. Confirm that every consumer has received the files and that any required logical recovery history is saved before purging; lagging consumers may need reseeding. Native WAL retention is a separate mechanism.

## Examples

```sql
PURGE BINARY LOGS TO 'mydb-bin.000010';
PURGE BINARY LOGS BEFORE '2026-08-01 00:00:00';
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

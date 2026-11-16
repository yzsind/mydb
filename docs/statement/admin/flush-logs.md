# `FLUSH LOGS`

## Description

Reopens/rotates all logs or a specified text log.

## Syntax

```ebnf
<flush-logs> ::= 'FLUSH'
                 ( 'LOGS'
                 | ( 'GENERAL' | 'SLOW' | 'ERROR' ) [ 'LOG' | 'LOGS' ] )
```

## Behavior, Privileges, and Limitations

- LOGS acts on all flushable logs in the current implementation; the specific forms flush only the corresponding log.
- FLUSH BINARY LOGS belongs to the binlog documentation and triggers a binlog rotate.
- File errors return an explicit administrative error; the RELOAD privilege is required.

## Privileges, Transaction, and Result Details

Requires global RELOAD and implicitly commits preceding DML. FLUSH LOGS rotates the active binary log when enabled and asks the active text-log service to flush its logs. GENERAL/SLOW/ERROR variants address the selected text-log kind. A disabled/uninitialized log has no active file to reopen; OK is not proof logging was enabled. File/rotation failure is an error after the commit boundary.

## Examples

```sql
FLUSH LOGS;
FLUSH SLOW LOGS;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)

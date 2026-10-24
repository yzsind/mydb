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

## Examples

```sql
FLUSH LOGS;
FLUSH SLOW LOGS;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)

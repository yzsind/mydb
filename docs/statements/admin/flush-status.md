# `FLUSH STATUS`

## Description

Resets process/session status counters that have a well-defined zero point.

## Syntax

```ebnf
<flush-status> ::= 'FLUSH STATUS'
```

## Behavior, Privileges, and Limitations

- Resets connection rejection/handshake/abort counters and resets Max_used_connections to the current open count. Foreground session status counters are merged into the global base and reset; many cumulative storage counters remain unchanged.
- The result can be verified by reading SHOW STATUS; the RELOAD privilege is required.

## Privileges, Transaction, and Result Details

Requires global RELOAD and implicitly commits preceding DML. It resets current foreground sessions' status values after folding them into the global base, so global totals are not simply zeroed. Max_used_connections resets to the current connection count. Storage/WAL counters can remain cumulative. The command itself contributes to command/flush statistics; capture the same metric scope before and after.

## Examples

```sql
FLUSH STATUS;
SHOW GLOBAL STATUS LIKE 'Max_used_connections';
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)

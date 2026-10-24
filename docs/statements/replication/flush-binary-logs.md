# `FLUSH BINARY LOGS`

## Description

Safely rotates the current binlog file.

## Syntax

```ebnf
<flush-binary-logs> ::= 'FLUSH BINARY LOGS'
```

## Behavior, Prerequisites, and Limitations

- Rotation happens under the commit gate, ensuring that transaction events are not torn apart.
- Requires the RELOAD privilege; the new file name can be checked with SHOW BINARY LOG STATUS.

## Examples

```sql
FLUSH BINARY LOGS;
SHOW BINARY LOG STATUS;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)

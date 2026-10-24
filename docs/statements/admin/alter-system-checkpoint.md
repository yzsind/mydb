# `ALTER SYSTEM CHECKPOINT`

## Description

Requests the storage engine to checkpoint immediately.

## Syntax

```ebnf
<checkpoint> ::= 'ALTER SYSTEM CHECKPOINT'
```

## Behavior, Privileges, and Limitations

- A checkpoint coordinates dirty pages with the WAL safe point; it is not equivalent to a backup.
- Requires administrative privileges; completion or failure is reported via OK or an explicit error.

## Examples

```sql
ALTER SYSTEM CHECKPOINT;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)

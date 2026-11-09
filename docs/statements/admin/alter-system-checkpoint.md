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

## Privileges, Transaction, and Result Details

Requires global SYSTEM_VARIABLES_ADMIN. The operation waits for the requested engine checkpoint work and returns OK or an error. It does not add the ordinary DDL implicit-commit call to the user's transaction; it cannot turn uncommitted business rows into a valid logical backup. Snapshot/WAL safety rules can retain history despite a checkpoint. Inspect SHOW ENGINE MYDB STATUS for durable/current LSN and checkpoint diagnostics.

## Examples

```sql
ALTER SYSTEM CHECKPOINT;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)

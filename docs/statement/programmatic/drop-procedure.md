# `DROP PROCEDURE`

## Description

Drops a stored procedure.

## Syntax

```ebnf
<drop-procedure> ::= 'DROP PROCEDURE' [ 'IF EXISTS' ] <qualified-name>
```

## Behavior and Limitations

- IF EXISTS turns a missing object into a Note.
- Dropping the definition does not undo data produced by earlier calls; the ALTER ROUTINE privilege is required.

## Context, Errors, and Lifecycle

Requires ALTER ROUTINE and implicitly commits preceding DML. IF EXISTS suppresses a missing definition (1305 without it). Automatic routine-grant cleanup follows automatic_sp_privileges. Removing a definition does not undo earlier calls; clients must not use DROP as cancellation of a business transaction.

## Examples

```sql
DROP PROCEDURE IF EXISTS transfer;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)

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

## Examples

```sql
DROP PROCEDURE IF EXISTS transfer;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)

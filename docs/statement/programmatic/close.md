# `CLOSE (stored program)`

## Description

Closes an open stored program cursor.

## Syntax

```ebnf
<close> ::= 'CLOSE' <cursor-name>
```

## Behavior and Limitations

- Closing releases the scan state; a subsequent FETCH or a repeated CLOSE returns an error.

## Context, Errors, and Lifecycle

A declared but unopened/already closed cursor returns 1326. CLOSE ends the scan, not the declaration, so a subsequent OPEN can execute the query again with current argument/local-variable values. Block exit closes its cursors automatically. Use [OPEN/FETCH/HANDLER together](../examples.md#complete-stored-cursor-example).

## Examples

```sql
CLOSE c;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

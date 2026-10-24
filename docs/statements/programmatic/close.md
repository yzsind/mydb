# `CLOSE (stored program)`

## Description

Closes an open stored program cursor.

## Syntax

```ebnf
<close> ::= 'CLOSE' <cursor-name>
```

## Behavior and Limitations

- Closing releases the scan state; a subsequent FETCH or a repeated CLOSE returns an error.

## Examples

```sql
CLOSE c;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

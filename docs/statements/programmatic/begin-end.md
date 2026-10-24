# `BEGIN ... END`

## Description

Establishes a compound statement block and local declaration scope inside a stored procedure, function, trigger, or event.

## Syntax

```ebnf
<compound> ::= [ <label> ':' ] 'BEGIN'
               { <declaration> } { <routine-statement> }
               'END' [ <label> ]
```

## Behavior and Limitations

- DECLARE must precede executable statements in the same block, and declarations are resolved in the legal order of variables, conditions, cursors, then handlers.
- Labels must match at the beginning and end; when the block is left, the scope of local variables, cursors, and handlers ends.

## Examples

```sql
main: BEGIN
  DECLARE v_count BIGINT DEFAULT 0;
  SELECT COUNT(*) INTO v_count FROM orders;
END main;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

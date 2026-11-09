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

## Context, Errors, and Lifecycle

This construct establishes scope and does not itself begin a data transaction. Local names/conditions/cursors can shadow outer block declarations; duplicate declarations and illegal declaration order are rejected. Labels are case-insensitive identifiers, can introduce BEGIN/LOOP/WHILE/REPEAT, and closing labels must match. Leaving the block restores outer bindings and closes block-local cursors. A function/trigger cannot use this block to bypass transaction restrictions. See [complete runnable body](../examples.md#complete-stored-cursor-example).

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

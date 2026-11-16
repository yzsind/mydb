# `LEAVE`

## Description

Exits a named LOOP, WHILE, REPEAT, or BEGIN block.

## Syntax

```ebnf
<leave> ::= 'LEAVE' <label>
```

## Behavior and Limitations

- The label must be visible in the current lexical enclosing scope.
- Leaving a block performs normal scope cleanup.

## Context, Errors, and Lifecycle

A label is a lexical enclosing block/loop name, not a session object. A missing target is 1308. LEAVE performs scope cleanup and does not itself COMMIT/ROLLBACK. To exit a whole procedure using LEAVE, label its outer block; RETURN with a value belongs to a function. See [block scope](begin-end.md).

## Examples

```sql
IF done THEN LEAVE read_loop; END IF;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

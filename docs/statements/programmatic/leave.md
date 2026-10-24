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

## Examples

```sql
IF done THEN LEAVE read_loop; END IF;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

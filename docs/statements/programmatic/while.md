# `WHILE`

## Description

Repeats stored program statements while the condition is true.

## Syntax

```ebnf
<while> ::= [ <label> ':' ] 'WHILE' <expr> 'DO'
            { <routine-statement> }
            'END WHILE' [ <label> ]
```

## Behavior and Limitations

- The condition is evaluated before each iteration; FALSE or NULL ends the loop.
- Labels are the targets for LEAVE/ITERATE.

## Examples

```sql
WHILE v_i < 10 DO
  SET v_i = v_i + 1;
END WHILE;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

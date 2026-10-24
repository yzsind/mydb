# `REPEAT`

## Description

Executes statements first, then ends when the UNTIL condition is true.

## Syntax

```ebnf
<repeat> ::= [ <label> ':' ] 'REPEAT'
             { <routine-statement> }
             'UNTIL' <expr>
             'END REPEAT' [ <label> ]
```

## Behavior and Limitations

- The body executes at least once; UNTIL TRUE ends the loop, FALSE/NULL continue it.

## Examples

```sql
REPEAT
  SET v_i = v_i + 1;
UNTIL v_i >= 10
END REPEAT;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

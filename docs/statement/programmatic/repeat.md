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

## Context, Errors, and Lifecycle

The body executes once before checking UNTIL. FALSE/NULL repeats and TRUE exits. ITERATE targets an enclosing visible loop; labels must match at END REPEAT. Initialize v_i before this example; it is not a top-level SQL statement. Resource limits/KILL still apply to nonterminating execution.

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

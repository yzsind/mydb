# `LOOP`

## Description

Repeats statements unconditionally; typically exited with LEAVE.

## Syntax

```ebnf
<loop> ::= [ <label> ':' ] 'LOOP'
           { <routine-statement> }
           'END LOOP' [ <label> ]
```

## Behavior and Limitations

- Has no natural completion condition. LEAVE, RETURN, a handler, an error or query cancellation must end execution.
- ITERATE can jump to the next iteration.

## Context, Errors, and Lifecycle

The body has no implicit exit condition. Use an initialized flag/counter or cursor NOT FOUND handler and a visible label; query cancellation can still end a running loop. The fetch-loop snippet assumes DECLARE/OPEN and a handler; [the complete example](../examples.md#complete-stored-cursor-example) includes setup and cleanup.

## Examples

```sql
read_loop: LOOP
  FETCH c INTO v_id;
  IF done THEN LEAVE read_loop; END IF;
END LOOP read_loop;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

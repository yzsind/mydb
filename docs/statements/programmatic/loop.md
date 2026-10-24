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

- Must be terminated by LEAVE, RETURN, a handler, or an error; otherwise it runs forever.
- ITERATE can jump to the next iteration.

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

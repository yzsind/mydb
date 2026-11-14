# `FETCH (stored program)`

## Description

Fetches the next row from a stored program cursor into local/user variables.

## Syntax

```ebnf
<fetch> ::= 'FETCH' [ 'NEXT' ] [ 'FROM' ] <cursor-name>
            'INTO' <routine-target> { ',' <routine-target> }
```

## Behavior and Limitations

- Reaching the end raises NOT FOUND; this is typically handled with a CONTINUE HANDLER.
- The number of targets must equal the cursor's column count.

## Context, Errors, and Lifecycle

At exhaustion, no new row is assigned and NOT FOUND (1329/02000) is raised; a CONTINUE HANDLER normally ends the loop. Fetching before OPEN/after CLOSE is 1326; a target count mismatch is 1328. Targets are assigned in order with declared-type conversion, so a later assignment error must not be mistaken for an untouched local-variable frame. See [complete loop](../examples.md#complete-stored-cursor-example).

## Examples

```sql
FETCH NEXT FROM c INTO v_id, v_total;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

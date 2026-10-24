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

## Examples

```sql
FETCH NEXT FROM c INTO v_id, v_total;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

# `GET DIAGNOSTICS`

## Description

Reads the current diagnostics area or the handler stack.

## Syntax

```ebnf
<get-diagnostics> ::= 'GET' [ 'CURRENT' | 'STACKED' ] 'DIAGNOSTICS'
                      ( <diag-assignment> { ',' <diag-assignment> }
                      | 'CONDITION' <expr>
                        <condition-assignment> { ',' <condition-assignment> } )
<diag-assignment> ::= <routine-target> '=' ( 'ROW_COUNT' | 'NUMBER' )
```

## Behavior and Limitations

- CURRENT reads the current diagnostics area; STACKED, valid only inside a handler, reads the condition being handled.
- Condition items support MYSQL_ERRNO, RETURNED_SQLSTATE, MESSAGE_TEXT, and source fields such as catalog/schema/table/column/cursor.

## Examples

```sql
GET DIAGNOSTICS v_rows = ROW_COUNT, v_count = NUMBER;
GET STACKED DIAGNOSTICS CONDITION 1
  v_errno = MYSQL_ERRNO, v_message = MESSAGE_TEXT;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

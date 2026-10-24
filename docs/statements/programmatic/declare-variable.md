# `DECLARE Variable`

## Description

Declares one or more local variables, optionally with a DEFAULT expression.

## Syntax

```ebnf
<declare-variable> ::= 'DECLARE' <identifier> { ',' <identifier> }
                       <data-type> [ 'DEFAULT' <expr> ]
```

## Behavior and Limitations

- Allowed only in the declaration section of a compound statement; without DEFAULT the initial value is NULL.
- The default expression is evaluated in the declaration-time context and converted to the declared type.

## Examples

```sql
DECLARE v_total DECIMAL(20,2) DEFAULT 0;
DECLARE v_done, v_retry BOOLEAN DEFAULT FALSE;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

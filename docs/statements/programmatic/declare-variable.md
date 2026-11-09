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

## Context, Errors, and Lifecycle

Names are local to the declaring block, with outer bindings restored on exit. A multi-name declaration evaluates its DEFAULT once and converts the copied value to each declared variable type; omission initializes NULL. Type conversion errors are not deferred until SELECT. Local variables/parameters bind by routine scope, so qualify table columns when a column has the same name. These snippets need a surrounding BEGIN ... END definition.

## Examples

```sql
DECLARE v_total DECIMAL(20,2) DEFAULT 0;
DECLARE v_done, v_retry BOOLEAN DEFAULT FALSE;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

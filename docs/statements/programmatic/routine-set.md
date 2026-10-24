# `SET (stored program)`

## Description

Assigns values to local variables, user variables, or permitted targets.

## Syntax

```ebnf
<routine-set> ::= 'SET' <routine-target> [ ':' ] '=' <expr>
                   { ',' <routine-target> [ ':' ] '=' <expr> }
<routine-target> ::= <identifier> | <user-variable>
```

## Behavior and Limitations

- Local variables resolve by block scope first; user variables start with @.
- Assignments perform type conversion; failure of any expression terminates that inner statement.

## Examples

```sql
SET v_total = v_total + p_amount, @last_total = v_total;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

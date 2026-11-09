# `SET (stored program)`

## Description

Assigns values to local variables, user variables, or permitted targets.

## Syntax

```ebnf
<routine-set> ::= 'SET' <routine-target> [ ':' ] '=' <expr>
                   { ',' <routine-target> [ ':' ] '=' <expr> }
<routine-target> ::= <identifier> | <user-variable>
                     | 'NEW' '.' <identifier>
```

## Behavior and Limitations

- Local variables resolve by block scope first; user variables start with @.
- Assignments perform type conversion; failure of any expression terminates that inner statement.

## Context, Errors, and Lifecycle

Local names bind to block scope before ordinary column names; @names bind to session user variables. SET NEW.column is available only for a writable nongenerated column in BEFORE INSERT/UPDATE. OLD is never writable; NEW is unavailable in DELETE. System variable forms such as SET @@session.sql_mode route through the general SET validator.

Assignments execute left to right; later expressions can read values written earlier. A later conversion/expression error does not imply earlier local/user-variable assignments were restored. User variables survive the routine and are not data-transaction state.

## Examples

```sql
SET v_total = v_total + p_amount, @last_total = v_total;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

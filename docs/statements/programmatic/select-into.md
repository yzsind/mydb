# `SELECT INTO (stored program)`

## Description

Assigns a single-row SELECT result to local or user variables.

## Syntax

```ebnf
<routine-select-into> ::= 'SELECT' <select-list>
                          'INTO' <routine-target> { ',' <routine-target> }
                          [ <select-tail> ]
```

## Behavior and Limitations

- The column count must equal the number of targets; more than one row returns 1172.
- Zero rows raise the NOT FOUND condition, which a handler can catch.

## Examples

```sql
SELECT total, state INTO v_total, v_state
FROM orders WHERE id = p_id;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

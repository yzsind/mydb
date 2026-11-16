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

## Context, Errors, and Lifecycle

No-data raises 1329/02000 and preserves previous targets; multiple rows raises 1172. The number of targets must match the returned row width. Values are converted and assigned in order, so conversion errors are different from source-cardinality errors. INTO can be placed in supported select-list/query-tail forms; use explicit SELECT aliases and LIMIT only if choosing one row is the intended logic. A handler can catch NOT FOUND. Source SELECT permissions follow the routine's security identity.

## Examples

```sql
SELECT total, state INTO v_total, v_state
FROM orders WHERE id = p_id;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)

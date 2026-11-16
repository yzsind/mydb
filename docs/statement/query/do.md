# `DO`

## Description

Evaluates one or more expressions and discards the results; it is well suited to invoking functions with session side effects without returning a result set.

## Syntax

```ebnf
<do> ::= 'DO' <expr> { ',' <expr> }
```

## Behavior and Limitations

- Expressions are evaluated in list order from left to right, and success returns an OK packet.
- Commonly used for `RELEASE_LOCK()`, `SLEEP()`, or expressions related to user-variable assignment.
- Expression, privilege, or memory errors terminate the statement; ordinary computed values are not sent to the client.
- Cannot contain FROM, WHERE, or a query projection; use SELECT when data must be read.

## Privileges, Results, and Boundaries

Returns OK, with expression warnings in the session diagnostics area. Scalar subqueries inside expressions can read data and need source SELECT privileges even though DO has no FROM clause. Called routines/functions retain their own privileges and side effects. A user variable, sequence value, named lock or remote operation is not restored by ordinary ROLLBACK.

## Examples

```sql
DO 1 + 1;
DO SLEEP(0.05), RELEASE_LOCK('nightly-job');
```

The client receives only a success status; if a function itself produces warnings, they can be inspected afterwards with `SHOW WARNINGS`.

## Related Statements

- [SELECT](select.md)
- [SHOW WARNINGS / ERRORS](../metadata/show-diagnostics.md)

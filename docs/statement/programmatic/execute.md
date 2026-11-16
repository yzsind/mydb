# `EXECUTE`

## Description

Executes a named prepared statement, supplying arguments from user variables.

## Syntax

```ebnf
<execute> ::= 'EXECUTE' <identifier>
              [ 'USING' <user-variable> { ',' <user-variable> } ]
```

## Behavior and Limitations

- The argument count must match exactly; values are bound according to the prepared parameter types and current session semantics.
- Plans/statement cache are reused, with safe recompilation when the schema or semantics are invalidated.
- Execution privileges are checked on every EXECUTE.

## Context, Errors, and Lifecycle

A missing name returns 1243 and a mismatched USING count returns 1210. An unset @parameter supplies SQL NULL. Values are bound as parameters, not interpolated into SQL. The prepared default database and parsing mode are preserved; runtime types and relevant session semantics are bound for execution. Privileges/revocations and catalog dependencies are rechecked; a stale plan is recompiled safely. Result/affected-row semantics are those of the target statement.

## Examples

```sql
SET @id = 42;
EXECUTE find_customer USING @id;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)

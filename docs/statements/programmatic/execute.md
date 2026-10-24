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

## Examples

```sql
SET @id = 42;
EXECUTE find_customer USING @id;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)

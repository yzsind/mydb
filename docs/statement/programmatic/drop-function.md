# `DROP FUNCTION`

## Description

Drops an SQL stored function.

## Syntax

```ebnf
<drop-function> ::= 'DROP FUNCTION' [ 'IF EXISTS' ] <qualified-name>
```

## Behavior and Limitations

- IF EXISTS controls whether a missing object is an error or a Note.
- After the drop, dependent queries report the function as missing when re-resolved; existing persisted data is unchanged.

## Context, Errors, and Lifecycle

Requires ALTER ROUTINE on the function and uses an implicit-commit DDL boundary. IF EXISTS changes missing-object diagnostics; a missing unqualified function normally returns 1305. Dependent query/function resolution is invalidated; recreating a name creates a new definition with its own captured settings/grants. This manages SQL stored functions, not arbitrary UDF shared libraries.

## Examples

```sql
DROP FUNCTION IF EXISTS add_tax;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)

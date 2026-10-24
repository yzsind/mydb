# `USE`

## Description

Switches the default database of the current connection.

## Syntax

```ebnf
<use> ::= 'USE' <identifier>
```

## Behavior, Privileges, and Results

- The database must exist and the account must have access to it.
- It only changes the name resolution context of the current session; it does not start or commit a transaction, and temporary tables still belong to the session.

## Examples

```sql
USE app;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)

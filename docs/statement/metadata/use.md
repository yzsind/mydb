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

## Errors and Session State

Unknown database returns 1049; insufficient database access returns 1044. A failed USE leaves the previous default database selected. Successful USE does not commit an active transaction; subsequent unqualified statements bind to the new default. Existing temporary tables remain session-owned and retain their original schema-qualified identity.

Use `SELECT DATABASE(), USER(), CURRENT_USER()` to inspect the selected database and login/authorization identities.

## Examples

```sql
USE app;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)

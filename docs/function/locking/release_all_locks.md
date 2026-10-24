# `RELEASE_ALL_LOCKS`

## Description

Releases all named locks held by the current connection.

## Syntax

```sql
RELEASE_ALL_LOCKS()
```

## Parameters

This function takes no arguments.

## Return Value

Returns an unsigned count of named-lock acquisitions released for the current session.

## Examples

```sql
SELECT RELEASE_ALL_LOCKS();
```

Result:

```text
0
```

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.
- User-level locks belong to the current connection and are released when it ends.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

# `CURRENT_ROLE`

## Description

Returns the roles activated in the current session.

## Syntax

```sql
CURRENT_ROLE()
```

## Parameters

This function takes no arguments.

## Return Value

Returns text describing the current session roles.

## Examples

```sql
SELECT CURRENT_ROLE();
```

Result: With no active roles, returns the string `NONE`. Activated roles change the returned role list; use SET ROLE NONE first when reproducing this case.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

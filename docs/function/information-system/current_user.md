# `CURRENT_USER`

## Description

Returns the authenticated account used for privilege checking.

## Syntax

```sql
CURRENT_USER()
```

## Parameters

This function takes no arguments.

## Return Value

Returns text describing the current session identity.

## Examples

```sql
SELECT CURRENT_USER();
```

Result: For the bootstrap account authenticated over loopback, returns `root@localhost`. Another matched authorization account changes this string.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

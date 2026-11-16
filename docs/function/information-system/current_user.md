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
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`DATABASE`](database.md), [`USER`](user.md), [`CURRENT_ROLE`](current_role.md).

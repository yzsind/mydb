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
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.
- User-level locks belong to the current connection and are released when it ends.

## Related Functions

[`GET_LOCK`](get_lock.md), [`RELEASE_LOCK`](release_lock.md), [`IS_FREE_LOCK`](is_free_lock.md), [`IS_USED_LOCK`](is_used_lock.md).

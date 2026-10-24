# `RELEASE_LOCK`

## Description

Releases the named lock held by the current connection.

## Syntax

```sql
RELEASE_LOCK(lock_name)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `lock_name` | Yes | String | The 1st argument; the user-level lock name |

## Return Value

Releases the named lock held by the current connection. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT RELEASE_LOCK('function-doc-example');
```

Result: returns 1 when the current connection holds the lock.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.
- User-level locks belong to the current connection and are released when it ends.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

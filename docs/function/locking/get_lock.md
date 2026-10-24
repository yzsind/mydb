# `GET_LOCK`

## Description

Acquires a user-level named lock within the given timeout.

## Syntax

```sql
GET_LOCK(lock_name, timeout)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `lock_name` | Yes | String | The 1st argument; the user-level lock name |
| `timeout` | Yes | Numeric or integer | The 2nd argument; the number of seconds to wait |

## Return Value

Acquires a user-level named lock within the given timeout. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT GET_LOCK('function-doc-example', 0);
```

Result: returns 1 when the lock is acquired; RELEASE_LOCK should be called after the example finishes.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.
- User-level locks belong to the current connection and are released when it ends.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

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
| `lock_name` | Yes | String | The user-level lock name |
| `timeout` | Yes | Numeric or integer | The number of seconds to wait |

## Return Value

Returns integer 1 when acquired/reentered, 0 when the wait times out, or an SQL error for invalid names/cancellation. A SQL NULL name is invalid. Timeout is in seconds, may be fractional, SQL NULL means zero wait and a negative timeout means an unbounded wait subject to cancellation.

## Examples

```sql
SELECT GET_LOCK('function-doc-example',0);
SELECT RELEASE_LOCK('function-doc-example');
```

Result:

```text
1
1
```

Assumes no other connection holds the example name. Always release successful acquisitions.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.
- User-level locks belong to the current connection and are released when it ends.

## Related Functions

[`RELEASE_LOCK`](release_lock.md), [`RELEASE_ALL_LOCKS`](release_all_locks.md), [`IS_FREE_LOCK`](is_free_lock.md), [`IS_USED_LOCK`](is_used_lock.md).

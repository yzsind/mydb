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
| `lock_name` | Yes | String | The user-level lock name |

## Return Value

Returns integer 1 after releasing one acquisition held by this connection, 0 if another connection owns the lock, or SQL NULL if no such lock exists. A SQL NULL name raises an invalid-name error.

## Examples

```sql
SELECT GET_LOCK('function-doc-example',0);
SELECT RELEASE_LOCK('function-doc-example');
SELECT RELEASE_LOCK('function-doc-example');
```

Result:

```text
1
1
NULL
```

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.
- User-level locks belong to the current connection and are released when it ends.

## Related Functions

[`GET_LOCK`](get_lock.md), [`RELEASE_ALL_LOCKS`](release_all_locks.md), [`IS_FREE_LOCK`](is_free_lock.md), [`IS_USED_LOCK`](is_used_lock.md).

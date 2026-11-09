# `IS_USED_LOCK`

## Description

Returns the connection ID holding a named lock.

## Syntax

```sql
IS_USED_LOCK(lock_name)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `lock_name` | Yes | String | The user-level lock name |

## Return Value

Returns the owning connection ID, or SQL NULL if the named lock is free. A SQL NULL name raises an invalid-name error.

## Examples

```sql
SELECT IS_USED_LOCK('function-doc-example');
```

Result:

```text
NULL
```

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`GET_LOCK`](get_lock.md), [`RELEASE_LOCK`](release_lock.md), [`RELEASE_ALL_LOCKS`](release_all_locks.md), [`IS_FREE_LOCK`](is_free_lock.md).

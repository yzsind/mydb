# `IS_FREE_LOCK`

## Description

Determines whether a named lock is currently free.

## Syntax

```sql
IS_FREE_LOCK(lock_name)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `lock_name` | Yes | String | The user-level lock name |

## Return Value

Returns integer 1 if the named lock is currently free, or 0 if held. A SQL NULL name raises an invalid-name error; this is only an observation, not an acquisition.

## Examples

```sql
SELECT IS_FREE_LOCK('function-doc-example');
```

Result:

```text
1
```

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`GET_LOCK`](get_lock.md), [`RELEASE_LOCK`](release_lock.md), [`RELEASE_ALL_LOCKS`](release_all_locks.md), [`IS_USED_LOCK`](is_used_lock.md).

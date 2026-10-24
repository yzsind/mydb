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
| `lock_name` | Yes | String | The 1st argument; the user-level lock name |

## Return Value

Returns integer 1 when the named lock is free, 0 when held, or NULL for an invalid name.

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
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

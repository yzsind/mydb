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
| `lock_name` | Yes | String | The 1st argument; the user-level lock name |

## Return Value

Returns the owning connection ID, or NULL when the named lock is free or invalid.

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
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

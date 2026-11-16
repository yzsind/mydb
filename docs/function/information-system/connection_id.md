# `CONNECTION_ID`

## Description

Returns the thread ID of the current connection.

## Syntax

```sql
CONNECTION_ID()
```

## Parameters

This function takes no arguments.

## Return Value

Returns the current connection identity as an unsigned integer. It is specific to this connection; it is not a query sequence number or a durable cross-restart identifier.

## Examples

```sql
SELECT CONNECTION_ID();
```

Result: returns the unsigned connection ID of the current connection.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`PS_CURRENT_THREAD_ID`](ps_current_thread_id.md), [`PS_THREAD_ID`](ps_thread_id.md), [`PS_THREAD_ACCOUNT`](ps_thread_account.md).

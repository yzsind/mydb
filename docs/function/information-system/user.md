# `USER`

## Description

Returns the `user@host` that established the current connection.

## Syntax

```sql
USER()
```

## Parameters

This function takes no arguments.

## Return Value

Returns text describing the current session identity.

## Examples

```sql
SELECT USER();
```

Result: For a root TCP connection to 127.0.0.1, returns `root@127.0.0.1`. The login name and peer address change with the connection; this need not equal CURRENT_USER.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

[`SESSION_USER`](session_user.md) and [`SYSTEM_USER`](system_user.md).

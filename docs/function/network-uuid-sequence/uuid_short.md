# `UUID_SHORT`

## Description

Generates a 64-bit counter value increasing across sessions of the current process.

## Syntax

```sql
UUID_SHORT()
```

## Parameters

This function takes no arguments.

## Return Value

Returns an unsigned 64-bit value derived from a fixed server byte of 1, the first-use Unix second, and a process-local counter. Counters are not persisted, and different instances or restarts in the same second can collide. Use an appropriate primary-key allocation scheme when durable or cross-instance uniqueness is required.

## Examples

```sql
SELECT UUID_SHORT();
```

Result: Returns an unsigned 64-bit ID, for example `102132488633057280`. The actual value depends on first-use time and the counter; this literal must not be reused as an expected fixed ID.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`UUID`](uuid.md), [`UUID_TO_BIN`](uuid_to_bin.md), [`BIN_TO_UUID`](bin_to_uuid.md), [`IS_UUID`](is_uuid.md).

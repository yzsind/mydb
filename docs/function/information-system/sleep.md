# `SLEEP`

## Description

Pauses execution for the specified number of seconds.

## Syntax

```sql
SLEEP(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Numeric or integer | The 1st argument; the input value |

## Return Value

Returns integer 0 when the requested sleep completes normally. Interruption follows the statement cancellation rules; duration is measured in seconds and may be fractional.

## Examples

```sql
SELECT SLEEP(2);
```

Result:

```text
0
```

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

# `SYSDATE`

## Description

Returns the current datetime at the time the function executes.

## Syntax

```sql
SYSDATE([fsp])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `fsp` | No | Integer 0..6 | Fractional-second digits, default 0. |

## Return Value

Returns a DATETIME using the actual evaluation-time clock in the session time zone. It is not frozen at statement start and ignores SET timestamp. fsp defaults to 0 and must be 0 through 6.

## Examples

```sql
SELECT SYSDATE();
```

Result: Returns the wall-clock DATETIME in the session time zone, for example `2026-10-22 12:34:56`. Unlike a fixed statement-start timestamp, it follows evaluation time.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`CURDATE`](curdate.md), [`CURTIME`](curtime.md), [`NOW`](now.md), [`UTC_DATE`](utc_date.md).

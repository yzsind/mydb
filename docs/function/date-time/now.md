# `NOW`

## Description

Returns the current datetime in the session time zone, with optional fractional-seconds precision.

## Syntax

```sql
NOW([fsp])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `fsp` | No | Integer from 0 to 6 | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT NOW(3);
```

Result: returns the current session time with 3 fractional-seconds digits; the exact value changes with each execution.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

[`CURRENT_TIMESTAMP`](current_timestamp.md), [`LOCALTIME`](localtime.md), and [`LOCALTIMESTAMP`](localtimestamp.md).

# `CURTIME`

## Description

Returns the current time in the session time zone.

## Syntax

```sql
CURTIME([fsp])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `fsp` | No | Integer from 0 to 6 | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT CURTIME(3);
```

Result: Returns the current session-zone TIME with three fractional digits, for example `12:34:56.789`; the clock value changes between statements.

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

[`CURRENT_TIME`](current_time.md).

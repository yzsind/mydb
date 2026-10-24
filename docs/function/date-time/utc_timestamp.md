# `UTC_TIMESTAMP`

## Description

Returns the current UTC datetime.

## Syntax

```sql
UTC_TIMESTAMP([fsp])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `fsp` | No | Integer from 0 to 6 | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT UTC_TIMESTAMP(3);
```

Result: Returns the current UTC DATETIME with the requested three fractional digits, for example `2026-10-22 04:34:56.789`; it changes with the clock.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

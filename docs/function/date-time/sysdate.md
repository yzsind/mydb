# `SYSDATE`

## Description

Returns the current datetime at the time the function executes.

## Syntax

```sql
SYSDATE()
```

## Parameters

This function takes no arguments.

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT SYSDATE();
```

Result: Returns the wall-clock DATETIME in the session time zone, for example `2026-10-22 12:34:56`. Unlike a fixed statement-start timestamp, it follows evaluation time.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

# `UTC_DATE`

## Description

Returns the current UTC date.

## Syntax

```sql
UTC_DATE()
```

## Parameters

This function takes no arguments.

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT UTC_DATE();
```

Result: Returns the current UTC DATE, for example `2026-10-22`; it can differ from the date in the session time zone.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

# `CURDATE`

## Description

Returns the current date in the session time zone.

## Syntax

```sql
CURDATE()
```

## Parameters

This function takes no arguments.

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT CURDATE();
```

Result: returns the current session date; the exact value varies with the execution date.

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

[`CURRENT_DATE`](current_date.md).

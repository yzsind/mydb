# `DATEDIFF`

## Description

Returns the number of days between two dates.

## Syntax

```sql
DATEDIFF(date1, date2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `date1` | Yes | date, time, datetime, or the corresponding text/numeric value | The 1st argument; the meaning is determined by the function syntax and description |
| `date2` | Yes | date, time, datetime, or the corresponding text/numeric value | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT DATEDIFF('2026-09-01', '2026-08-30');
```

Result: 2

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

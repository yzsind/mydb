# `PERIOD_DIFF`

## Description

Returns the number of months between two period values.

## Syntax

```sql
PERIOD_DIFF(period1, period2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `period1` | Yes | date, time, datetime, or the corresponding text/numeric value | The 1st argument; the meaning is determined by the function syntax and description |
| `period2` | Yes | date, time, datetime, or the corresponding text/numeric value | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT PERIOD_DIFF(202609, 202608);
```

Result: 1

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

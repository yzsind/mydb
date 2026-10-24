# `TIMESTAMPADD`

## Description

Adds an integer interval to a datetime in the specified unit.

## Syntax

```sql
TIMESTAMPADD(unit, interval, datetime_expr)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `unit` | Yes | datetime unit keyword | The interval unit |
| `interval` | Yes | integer | The number of intervals to add |
| `datetime_expr` | Yes | date or datetime | The starting value |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT TIMESTAMPADD(DAY, 2, '2026-08-30');
```

Result: `2026-09-01`; a date-only input with a DAY interval produces a date here.

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

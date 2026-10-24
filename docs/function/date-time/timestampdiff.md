# `TIMESTAMPDIFF`

## Description

Returns the difference between two datetimes in the specified unit.

## Syntax

```sql
TIMESTAMPDIFF(unit, datetime_expr1, datetime_expr2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `unit` | Yes | datetime unit keyword | The unit of the result |
| `datetime_expr1` | Yes | date or datetime | The starting value |
| `datetime_expr2` | Yes | date or datetime | The ending value |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT TIMESTAMPDIFF(DAY, '2026-08-30', '2026-09-01');
```

Result: 2

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

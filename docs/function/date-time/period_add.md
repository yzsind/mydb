# `PERIOD_ADD`

## Description

Adds a specified number of months to a `YYYYMM` or `YYMM` period value.

## Syntax

```sql
PERIOD_ADD(period, months)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `period` | Yes | date, time, datetime, or the corresponding text/numeric value | The 1st argument; the meaning is determined by the function syntax and description |
| `months` | Yes | date, time, datetime, or the corresponding text/numeric value | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT PERIOD_ADD(202608, 1);
```

Result: 202609

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

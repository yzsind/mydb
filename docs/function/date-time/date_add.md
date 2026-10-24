# `DATE_ADD`

## Description

Adds an interval to a date or datetime.

## Syntax

```sql
DATE_ADD(date, INTERVAL expr unit)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `date` | Yes | date or datetime | The starting value |
| `INTERVAL expr unit` | Yes | interval expression | The amount and unit of time to add |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT DATE_ADD('2026-08-30', INTERVAL 1 DAY);
```

Result: 2026-08-31

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

[`ADDDATE`](adddate.md).

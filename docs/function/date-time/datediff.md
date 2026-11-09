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
| `date1` | Yes | date, time, datetime, or the corresponding text/numeric value | First date; its time portion is ignored. |
| `date2` | Yes | date, time, datetime, or the corresponding text/numeric value | Second date; its time portion is ignored. |

## Return Value

Returns the signed integer day count date1 minus date2. Only the calendar dates participate; time-of-day is ignored. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT DATEDIFF('2026-09-01', '2026-08-30');
```

Result: 2

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

- For example DATEDIFF('2026-09-01 00:00:00','2026-08-30 23:59:59') returns 2; elapsed 24-hour periods are not counted.

## Related Functions

[`DATE_ADD`](date_add.md), [`DATE_SUB`](date_sub.md), [`ADDTIME`](addtime.md), [`SUBTIME`](subtime.md).

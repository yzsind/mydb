# `WEEKOFYEAR`

## Description

Returns the ISO-style week number of the year for a date.

## Syntax

```sql
WEEKOFYEAR(date [, mode])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `date` | Yes | date, time, datetime, or the corresponding text/numeric value | Date or datetime to process. |
| `mode` | No | Compatibility argument | Evaluated, but the calculation always uses mode 3; a supplied SQL NULL returns NULL |

## Return Value

Returns the mode-3 week number (Monday first, range 1..53, first week has at least four days). SQL NULL input returns NULL; invalid temporal values follow the SQL-mode diagnostic rules. The ordinary MySQL spelling takes one argument; mydb also accepts a second argument but does not use it to select another mode. Use WEEK(date, mode) for selectable modes.

## Examples

```sql
SELECT WEEKOFYEAR('2026-08-30 12:34:56');
```

Result:

```text
35
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

- See [week modes](README.md#week-modes) for the first-day/first-week rules and cross-year behavior.

## Related Functions

[`DAYOFWEEK`](dayofweek.md), [`WEEKDAY`](weekday.md), [`WEEK`](week.md), [`YEARWEEK`](yearweek.md).

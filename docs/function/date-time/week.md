# `WEEK`

## Description

Returns the week number of a date with an optional mode.

## Syntax

```sql
WEEK(date [, mode])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `date` | Yes | date | The source date |
| `mode` | No | integer from 0 to 7 | The week-number mode |

## Return Value

Returns an integer week number using mode 0..7. When mode is omitted it uses default_week_format. Modes with a zero-based week range can return week 0 at the start of the year. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT WEEK('2026-08-30 12:34:56');
```

Result:

```text
35
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

- See [week modes](README.md#week-modes) for the first-day/first-week rules and cross-year behavior.

## Related Functions

[`DAYOFWEEK`](dayofweek.md), [`WEEKDAY`](weekday.md), [`WEEKOFYEAR`](weekofyear.md), [`YEARWEEK`](yearweek.md).

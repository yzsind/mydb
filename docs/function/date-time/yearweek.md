# `YEARWEEK`

## Description

Returns an integer composed of the year and week number.

## Syntax

```sql
YEARWEEK(date [, mode])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `date` | Yes | date | The source date |
| `mode` | No | integer from 0 to 7 | The week-number mode |

## Return Value

Returns an integer week-year times 100 plus week number. The week-year can differ from YEAR(date) near New Year. The default mode is 0; the result uses a week-year rather than returning week zero. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT YEARWEEK('2026-08-30 12:34:56');
```

Result:

```text
202635
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

- See [week modes](README.md#week-modes) for the first-day/first-week rules and cross-year behavior.

## Related Functions

[`DAYOFWEEK`](dayofweek.md), [`WEEKDAY`](weekday.md), [`WEEK`](week.md), [`WEEKOFYEAR`](weekofyear.md).

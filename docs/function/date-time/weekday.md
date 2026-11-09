# `WEEKDAY`

## Description

Returns the weekday index for a date, with Monday as 0.

## Syntax

```sql
WEEKDAY(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

Returns the weekday in 0..6, Monday=0 as an integer. SQL NULL returns NULL; invalid temporal conversions produce the applicable warning/error or NULL.

## Examples

```sql
SELECT WEEKDAY('2026-08-30 12:34:56');
```

Result:

```text
6
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`DAYOFWEEK`](dayofweek.md), [`WEEK`](week.md), [`WEEKOFYEAR`](weekofyear.md), [`YEARWEEK`](yearweek.md).

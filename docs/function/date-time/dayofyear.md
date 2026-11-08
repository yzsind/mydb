# `DAYOFYEAR`

## Description

Returns the day of the year for a date.

## Syntax

```sql
DAYOFYEAR(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

Returns the day-of-year in 1..366 as an integer. SQL NULL returns NULL; invalid temporal conversions produce the applicable warning/error or NULL.

## Examples

```sql
SELECT DAYOFYEAR('2026-08-30 12:34:56');
```

Result:

```text
242
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`DAYOFMONTH`](dayofmonth.md), [`MONTH`](month.md), [`YEAR`](year.md), [`QUARTER`](quarter.md).

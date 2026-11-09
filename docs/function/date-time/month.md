# `MONTH`

## Description

Returns the month of a date.

## Syntax

```sql
MONTH(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

Returns the month in 0..12, where zero is possible for an incomplete date as an integer. SQL NULL returns NULL; invalid temporal conversions produce the applicable warning/error or NULL.

## Examples

```sql
SELECT MONTH('2026-08-30 12:34:56');
```

Result:

```text
8
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`DAYOFMONTH`](dayofmonth.md), [`DAYOFYEAR`](dayofyear.md), [`YEAR`](year.md), [`QUARTER`](quarter.md).

# `YEAR`

## Description

Returns the year of a date.

## Syntax

```sql
YEAR(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

Returns the calendar year as an integer. SQL NULL returns NULL; invalid temporal conversions produce the applicable warning/error or NULL.

## Examples

```sql
SELECT YEAR('2026-08-30 12:34:56');
```

Result:

```text
2026
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`DAYOFMONTH`](dayofmonth.md), [`DAYOFYEAR`](dayofyear.md), [`MONTH`](month.md), [`QUARTER`](quarter.md).

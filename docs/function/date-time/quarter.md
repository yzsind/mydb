# `QUARTER`

## Description

Returns the quarter of the year for a date.

## Syntax

```sql
QUARTER(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

Returns the quarter in 1..4 for a complete date as an integer. SQL NULL returns NULL; invalid temporal conversions produce the applicable warning/error or NULL.

## Examples

```sql
SELECT QUARTER('2026-08-30 12:34:56');
```

Result:

```text
3
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`DAYOFMONTH`](dayofmonth.md), [`DAYOFYEAR`](dayofyear.md), [`MONTH`](month.md), [`YEAR`](year.md).

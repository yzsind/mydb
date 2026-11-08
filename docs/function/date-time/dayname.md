# `DAYNAME`

## Description

Returns the localized weekday name for a date.

## Syntax

```sql
DAYNAME(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

Returns the weekday name as text in the session lc_time_names locale. SQL NULL returns NULL; invalid temporal input produces the applicable diagnostic.

## Examples

```sql
SELECT DAYNAME('2026-08-30 12:34:56');
```

Result:

```text
Sunday
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`DAYOFMONTH`](dayofmonth.md), [`DAYOFYEAR`](dayofyear.md), [`MONTH`](month.md), [`YEAR`](year.md).

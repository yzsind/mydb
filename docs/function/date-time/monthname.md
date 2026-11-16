# `MONTHNAME`

## Description

Returns the localized month name for a date.

## Syntax

```sql
MONTHNAME(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

Returns the month name as text in the session lc_time_names locale. SQL NULL returns NULL; invalid temporal input produces the applicable diagnostic.

## Examples

```sql
SELECT MONTHNAME('2026-08-30 12:34:56');
```

Result:

```text
August
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`DAYOFMONTH`](dayofmonth.md), [`DAYOFYEAR`](dayofyear.md), [`MONTH`](month.md), [`YEAR`](year.md).

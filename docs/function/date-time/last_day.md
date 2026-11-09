# `LAST_DAY`

## Description

Returns the last day of the month for the argument.

## Syntax

```sql
LAST_DAY(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

Returns a DATE value; SQL NULL input returns NULL. Invalid dates follow the current sql_mode and temporal-conversion rules.

## Examples

```sql
SELECT LAST_DAY('2026-08-30 12:34:56');
```

Result:

```text
2026-08-31
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`DAYOFMONTH`](dayofmonth.md), [`DAYOFYEAR`](dayofyear.md), [`MONTH`](month.md), [`YEAR`](year.md).

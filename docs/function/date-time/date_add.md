# `DATE_ADD`

## Description

Adds an interval to a date or datetime.

## Syntax

```sql
DATE_ADD(date, INTERVAL expr unit)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `date` | Yes | date or datetime | The starting value |
| `INTERVAL expr unit` | Yes | interval expression | The amount and unit of time to add |

## Return Value

Returns the temporal value after interval addition. A date-only value with a calendar-date interval can remain a DATE; time-bearing units require a time/datetime result. Month arithmetic clamps an unavailable day to the last day of the target month. Invalid/overflowing results produce temporal diagnostics. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT DATE_ADD('2026-08-30', INTERVAL 1 DAY);
```

Result: 2026-08-31

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

- See [interval units](README.md#interval-units) for supported units, compound values, and operator restrictions.

## Related Functions

[`ADDDATE`](adddate.md).

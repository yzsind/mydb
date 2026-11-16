# `TIMESTAMPADD`

## Description

Adds an integer interval to a datetime in the specified unit.

## Syntax

```sql
TIMESTAMPADD(unit, interval, datetime_expr)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `unit` | Yes | datetime unit keyword | The interval unit |
| `interval` | Yes | integer | The number of intervals to add |
| `datetime_expr` | Yes | date or datetime | The starting value |

## Return Value

Returns the temporal value after adding the integer interval in unit. Date-only inputs with date units can produce DATE; time-bearing units require datetime precision. Month/year addition clamps unavailable month-end days. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT TIMESTAMPADD(DAY, 2, '2026-08-30');
```

Result: `2026-09-01`; a date-only input with a DAY interval produces a date here.

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

- See [interval units](README.md#interval-units) for supported units, compound values, and operator restrictions.

## Related Functions

[`DATE_ADD`](date_add.md), [`DATE_SUB`](date_sub.md), [`ADDTIME`](addtime.md), [`SUBTIME`](subtime.md).

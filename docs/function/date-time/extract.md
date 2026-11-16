# `EXTRACT`

## Description

Extracts a datetime field in the specified unit.

## Syntax

```sql
EXTRACT(unit FROM temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `unit` | Yes | datetime unit keyword | The field or compound unit to extract |
| `temporal_value` | Yes | date, time, or datetime | The source value |

## Return Value

Returns an integer field or packed compound-unit value from the temporal input. Compound values concatenate the named fields, for example YEAR_MONTH is YYYYMM. Negative durations retain their applicable sign. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT EXTRACT(YEAR FROM '2026-08-30');
```

Result: 2026

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

- See [interval units](README.md#interval-units) for supported units, compound values, and operator restrictions.

## Related Functions

[`DATE`](date.md), [`TIME`](time.md), [`TIMESTAMP`](timestamp.md).

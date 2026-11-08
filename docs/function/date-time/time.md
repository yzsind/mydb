# `TIME`

## Description

Extracts the time part of a datetime.

## Syntax

```sql
TIME(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

Returns a TIME value, preserving the applicable fractional precision. SQL NULL input returns NULL; invalid or out-of-range times follow the temporal-conversion rules.

## Examples

```sql
SELECT TIME('2026-08-30 12:34:56'), TIME('2026-08-30 12:34:56.123');
```

Result:

```text
12:34:56	12:34:56.123
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`DATE`](date.md), [`TIMESTAMP`](timestamp.md), [`EXTRACT`](extract.md).

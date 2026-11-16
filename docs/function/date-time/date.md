# `DATE`

## Description

Extracts the date part of a date or datetime.

## Syntax

```sql
DATE(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

Returns a DATE value; SQL NULL input returns NULL. Invalid dates follow the current sql_mode and temporal-conversion rules.

## Examples

```sql
SELECT DATE('2026-08-30 12:34:56');
```

Result:

```text
2026-08-30
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`TIME`](time.md), [`TIMESTAMP`](timestamp.md), [`EXTRACT`](extract.md).

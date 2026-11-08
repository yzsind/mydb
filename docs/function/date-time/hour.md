# `HOUR`

## Description

Returns the hour part of a time value.

## Syntax

```sql
HOUR(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

Returns the hour component, including hours greater than 23 for durations as an integer. SQL NULL returns NULL; invalid temporal conversions produce the applicable warning/error or NULL.

## Examples

```sql
SELECT HOUR('2026-08-30 12:34:56');
```

Result:

```text
12
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`MINUTE`](minute.md), [`SECOND`](second.md), [`MICROSECOND`](microsecond.md).

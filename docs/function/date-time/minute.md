# `MINUTE`

## Description

Returns the minute part of a time value.

## Syntax

```sql
MINUTE(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

Returns the minute component in 0..59 as an integer. SQL NULL returns NULL; invalid temporal conversions produce the applicable warning/error or NULL.

## Examples

```sql
SELECT MINUTE('2026-08-30 12:34:56');
```

Result:

```text
34
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`HOUR`](hour.md), [`SECOND`](second.md), [`MICROSECOND`](microsecond.md).

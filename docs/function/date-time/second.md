# `SECOND`

## Description

Returns the second part of a time value.

## Syntax

```sql
SECOND(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

Returns the whole-second component in 0..59 as an integer. SQL NULL returns NULL; invalid temporal conversions produce the applicable warning/error or NULL.

## Examples

```sql
SELECT SECOND('2026-08-30 12:34:56');
```

Result:

```text
56
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`HOUR`](hour.md), [`MINUTE`](minute.md), [`MICROSECOND`](microsecond.md).

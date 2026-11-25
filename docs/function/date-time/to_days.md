# `TO_DAYS`

## Description

Converts a date to a day number.

## Syntax

```sql
TO_DAYS(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

Returns an integer extracted or calculated from the temporal input. SQL NULL input returns NULL; invalid temporal values produce NULL and the applicable SQL-mode diagnostic.

This is a calendar day number, not a Unix epoch day count. FROM_DAYS maps
numbers at or below 365 to the zero date, so the two functions are not inverse
operations for year-0000 dates.

## Examples

```sql
SELECT TO_DAYS('2026-08-30 12:34:56');
```

Result:

```text
740223
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`FROM_DAYS`](from_days.md), [`TO_SECONDS`](to_seconds.md).

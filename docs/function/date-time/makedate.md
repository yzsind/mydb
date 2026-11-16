# `MAKEDATE`

## Description

Constructs a date from a year and day-of-year number.

## Syntax

```sql
MAKEDATE(year, dayofyear)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `year` | Yes | Numeric expression | Calendar year. |
| `dayofyear` | Yes | Numeric expression | One-based day of the requested year; must be positive. |

## Return Value

Returns a DATE for the given year and one-based day-of-year. Day-of-year beyond that year rolls into the following year; nonpositive day-of-year or an out-of-range result returns NULL. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT MAKEDATE(2026, 242);
```

Result: 2026-08-30

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`MAKETIME`](maketime.md), [`SEC_TO_TIME`](sec_to_time.md), [`TIME_TO_SEC`](time_to_sec.md).

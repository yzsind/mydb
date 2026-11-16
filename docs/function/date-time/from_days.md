# `FROM_DAYS`

## Description

Converts a day number to a date.

## Syntax

```sql
FROM_DAYS(day_number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `day_number` | Yes | Integer | MySQL day number; not a DATE or a Unix timestamp. |

## Return Value

Returns a DATE derived from the MySQL day number. Nonpositive day numbers return the zero date; out-of-calendar-range results return NULL with the applicable diagnostic. This is not a Unix epoch day count. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT FROM_DAYS(740223);
```

Result:

```text
2026-08-30
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`TO_DAYS`](to_days.md), [`TO_SECONDS`](to_seconds.md).

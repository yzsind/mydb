# `DATE_FORMAT`

## Description

Formats a datetime according to a format string.

## Syntax

```sql
DATE_FORMAT(date, format)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `date` | Yes | date, time, datetime, or the corresponding text/numeric value | Date or datetime to process. |
| `format` | Yes | String | Format string; see the category format-specifier table. |

## Return Value

Returns formatted text, using lc_time_names for localized month/weekday names. Format specifiers and literal escaping are listed in the category reference. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT DATE_FORMAT('2026-08-30', '%Y/%m/%d');
```

Result: 2026/08/30

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

- See [format specifiers](README.md#format-specifiers) and [predefined formats](README.md#predefined-formats). A SQL NULL format argument returns NULL.

## Related Functions

[`TIME_FORMAT`](time_format.md), [`STR_TO_DATE`](str_to_date.md), [`GET_FORMAT`](get_format.md).

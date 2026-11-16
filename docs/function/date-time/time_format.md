# `TIME_FORMAT`

## Description

Formats a time value according to a format string.

## Syntax

```sql
TIME_FORMAT(time, format)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `time` | Yes | date, time, datetime, or the corresponding text/numeric value | Time or duration to process. |
| `format` | Yes | String | Format string; see the category format-specifier table. |

## Return Value

Returns text formatted from a signed TIME/duration. Hour fields can exceed 23; calendar-dependent specifiers are not supported for a time-only value. See the format table. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT TIME_FORMAT('12:34:56', '%H:%i');
```

Result: 12:34

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

- See [format specifiers](README.md#format-specifiers) and [predefined formats](README.md#predefined-formats). A SQL NULL format argument returns NULL.

## Related Functions

[`DATE_FORMAT`](date_format.md), [`STR_TO_DATE`](str_to_date.md), [`GET_FORMAT`](get_format.md).

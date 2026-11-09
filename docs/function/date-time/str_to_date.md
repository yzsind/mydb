# `STR_TO_DATE`

## Description

Parses a datetime string according to a format string.

## Syntax

```sql
STR_TO_DATE(str, format)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |
| `format` | Yes | String | Format string; see the category format-specifier table. |

## Return Value

Returns DATE, TIME or DATETIME according to the date/time fields present in format. Text that cannot be parsed returns NULL with the applicable temporal diagnostic. Incomplete/zero fields and strict write contexts obey SQL mode. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT STR_TO_DATE('30/08/2026', '%d/%m/%Y');
```

Result: 2026-08-30

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

- See [format specifiers](README.md#format-specifiers) and [predefined formats](README.md#predefined-formats). A SQL NULL format argument returns NULL.

## Related Functions

[`DATE_FORMAT`](date_format.md), [`TIME_FORMAT`](time_format.md), [`GET_FORMAT`](get_format.md).

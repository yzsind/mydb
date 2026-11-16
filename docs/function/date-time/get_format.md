# `GET_FORMAT`

## Description

Returns a date or time format string in the specified locale style.

## Syntax

```sql
GET_FORMAT(type, locale)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `type` | Yes | DATE, TIME, DATETIME, or TIMESTAMP | The format type |
| `locale` | Yes | USA, JIS, ISO, EUR, or INTERNAL | A predefined format style |

## Return Value

Returns one of the predefined format strings in the category table, or NULL for an unknown style. The style selects a pattern, not lc_time_names or a time zone. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT GET_FORMAT(DATE, 'USA');
```

Result: %m.%d.%Y

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

- See [format specifiers](README.md#format-specifiers) and [predefined formats](README.md#predefined-formats). A SQL NULL format argument returns NULL.

## Related Functions

[`DATE_FORMAT`](date_format.md), [`TIME_FORMAT`](time_format.md), [`STR_TO_DATE`](str_to_date.md).

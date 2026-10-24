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

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT GET_FORMAT(DATE, 'USA');
```

Result: %m.%d.%Y

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

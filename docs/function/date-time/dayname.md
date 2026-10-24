# `DAYNAME`

## Description

Returns the localized weekday name for a date.

## Syntax

```sql
DAYNAME(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns the localized weekday/month name as text; the example uses the default English locale. SQL NULL input returns NULL.

## Examples

```sql
SELECT DAYNAME('2026-08-30 12:34:56');
```

Result:

```text
Sunday
```

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

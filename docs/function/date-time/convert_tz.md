# `CONVERT_TZ`

## Description

Converts a datetime between two time zones.

## Syntax

```sql
CONVERT_TZ(datetime, from_tz, to_tz)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `datetime` | Yes | date, time, datetime, or the corresponding text/numeric value | The 1st argument; the meaning is determined by the function syntax and description |
| `from_tz` | Yes | Time-zone string | Source fixed offset or supported IANA zone name |
| `to_tz` | Yes | Time-zone string | Destination fixed offset or supported IANA zone name |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT CONVERT_TZ('2026-08-30 12:00:00', '+00:00', '+08:00');
```

Result: 2026-08-30 20:00:00

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.
- Future conversions support mydb's extended UTC TIMESTAMP domain through
  year 9999, including named-zone future DST rules. UTC or target-wall-clock
  overflow beyond year 9999 returns `NULL`. Pre-epoch conversions retain the
  existing behavior of returning the input unchanged.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

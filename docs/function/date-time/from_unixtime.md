# `FROM_UNIXTIME`

## Description

Converts a Unix timestamp to a datetime in the session time zone, with optional formatted output.

## Syntax

```sql
FROM_UNIXTIME(unix_timestamp [, format])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `unix_timestamp` | Yes | numeric value | The number of Unix seconds |
| `format` | No | format string | When provided, returns formatted text |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT FROM_UNIXTIME(0);
```

Result: returns the Unix epoch as a datetime in the session time zone.

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.
- mydb accepts Unix seconds from zero through `253402300799.999999`.
  Use exact decimal or text input to preserve microseconds near year 9999.
  Rounding into year 10000, or a session time-zone conversion beyond calendar
  years 0000–9999, returns `NULL`, including when a format argument is present.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

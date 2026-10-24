# `MAKETIME`

## Description

Constructs a time value from hours, minutes, and seconds.

## Syntax

```sql
MAKETIME(hour, minute, second)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `hour` | Yes | date, time, datetime, or the corresponding text/numeric value | The 1st argument; the meaning is determined by the function syntax and description |
| `minute` | Yes | date, time, datetime, or the corresponding text/numeric value | The 2nd argument; the meaning is determined by the function syntax and description |
| `second` | Yes | date, time, datetime, or the corresponding text/numeric value | The 3rd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT MAKETIME(12, 34, 56);
```

Result: 12:34:56

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

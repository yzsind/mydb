# `WEEKOFYEAR`

## Description

Returns the ISO-style week number of the year for a date.

## Syntax

```sql
WEEKOFYEAR(date [, mode])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `date` | Yes | date, time, datetime, or the corresponding text/numeric value | The 1st argument; the meaning is determined by the function syntax and description |
| `mode` | No | Compatibility argument | Evaluated, but the calculation always uses mode 3; a supplied SQL NULL returns NULL |

## Return Value

Returns the mode-3 week number (Monday first, range 1..53, first week has at least four days). SQL NULL input returns NULL; invalid temporal values follow the SQL-mode diagnostic rules. The ordinary MySQL spelling takes one argument; mydb also accepts a second argument but does not use it to select another mode. Use WEEK(date, mode) for selectable modes.

## Examples

```sql
SELECT WEEKOFYEAR('2026-08-30 12:34:56');
```

Result:

```text
35
```

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

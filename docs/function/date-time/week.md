# `WEEK`

## Description

Returns the week number of a date with an optional mode.

## Syntax

```sql
WEEK(date [, mode])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `date` | Yes | date | The source date |
| `mode` | No | integer from 0 to 7 | The week-number mode |

## Return Value

Returns an integer extracted or calculated from the temporal input. SQL NULL input returns NULL; invalid temporal values produce NULL and the applicable SQL-mode diagnostic.

## Examples

```sql
SELECT WEEK('2026-08-30 12:34:56');
```

Result:

```text
35
```

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

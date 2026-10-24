# `TIME`

## Description

Extracts the time part of a datetime.

## Syntax

```sql
TIME(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a TIME value, preserving the applicable fractional precision. SQL NULL input returns NULL; invalid or out-of-range times follow the temporal-conversion rules.

## Examples

```sql
SELECT TIME('2026-08-30 12:34:56');
```

Result:

```text
12:34:56.000000
```

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

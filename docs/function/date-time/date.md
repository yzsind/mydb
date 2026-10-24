# `DATE`

## Description

Extracts the date part of a date or datetime.

## Syntax

```sql
DATE(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a DATE value; SQL NULL input returns NULL. Invalid dates follow the current sql_mode and temporal-conversion rules.

## Examples

```sql
SELECT DATE('2026-08-30 12:34:56');
```

Result:

```text
2026-08-30
```

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

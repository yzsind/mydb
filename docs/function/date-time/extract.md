# `EXTRACT`

## Description

Extracts a datetime field in the specified unit.

## Syntax

```sql
EXTRACT(unit FROM temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `unit` | Yes | datetime unit keyword | The field or compound unit to extract |
| `temporal_value` | Yes | date, time, or datetime | The source value |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT EXTRACT(YEAR FROM '2026-08-30');
```

Result: 2026

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

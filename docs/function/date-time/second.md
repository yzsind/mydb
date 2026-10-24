# `SECOND`

## Description

Returns the second part of a time value.

## Syntax

```sql
SECOND(temporal_value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | Yes | Date, time, datetime, or matching text/numeric value | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns an integer extracted or calculated from the temporal input. SQL NULL input returns NULL; invalid temporal values produce NULL and the applicable SQL-mode diagnostic.

## Examples

```sql
SELECT SECOND('2026-08-30 12:34:56');
```

Result:

```text
56
```

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

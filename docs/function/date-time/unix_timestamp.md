# `UNIX_TIMESTAMP`

## Description

Returns the current Unix timestamp, or converts a datetime to a Unix timestamp.

## Syntax

```sql
UNIX_TIMESTAMP([datetime])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | No | date, time, datetime, or the corresponding text/numeric value | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00');
```

Result: the result depends on the session time zone.

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.
- mydb supports nonnegative epoch seconds through `253402300799.999999`
  (`9999-12-31 23:59:59.999999` UTC). A TIMESTAMP argument is already UTC;
  DATETIME and text arguments use the session time zone. Exact decimal output
  preserves microseconds. Out-of-range datetime arguments return zero.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

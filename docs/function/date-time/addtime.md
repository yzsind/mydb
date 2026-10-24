# `ADDTIME`

## Description

Adds a time value to a time or datetime.

## Syntax

```sql
ADDTIME(expr1, expr2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr1` | Yes | Any SQL expression | The 1st argument; the meaning is determined by the function syntax and description |
| `expr2` | Yes | Any SQL expression | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a date, time, datetime, string, or numeric value; invalid input produces `NULL`, a warning, or an error according to the current SQL mode.

## Examples

```sql
SELECT ADDTIME('12:00:00', '01:30:00');
```

Result: 13:30:00

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

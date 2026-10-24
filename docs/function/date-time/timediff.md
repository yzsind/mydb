# `TIMEDIFF`

## Description

Returns the difference between two time or datetime values.

## Syntax

```sql
TIMEDIFF(expr1, expr2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr1` | Yes | Any SQL expression | The 1st argument; the meaning is determined by the function syntax and description |
| `expr2` | Yes | Any SQL expression | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a TIME value, preserving the applicable fractional precision. SQL NULL input returns NULL; invalid or out-of-range times follow the temporal-conversion rules.

## Examples

```sql
SELECT TIMEDIFF('2026-08-30 12:34:56', '2026-08-30 12:34:56');
```

Result:

```text
00:00:00
```

## Notes

- Date and time functions follow the session `time_zone`, `sql_mode`, and fractional-seconds precision settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

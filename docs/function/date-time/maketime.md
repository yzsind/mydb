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
| `hour` | Yes | Numeric expression | Signed hours; values can exceed 23. |
| `minute` | Yes | Numeric expression | Minute component, 0 through 59. |
| `second` | Yes | Numeric expression | Second component, including fractional seconds; must be less than 60. |

## Return Value

Returns a signed TIME value built from hours, minutes and seconds. Hours can exceed 23; minutes must be 0..59 and seconds in [0,60). Invalid components return NULL; TIME-range overflow uses clamping/diagnostics. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT MAKETIME(12, 34, 56);
```

Result: 12:34:56

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`MAKEDATE`](makedate.md), [`SEC_TO_TIME`](sec_to_time.md), [`TIME_TO_SEC`](time_to_sec.md).

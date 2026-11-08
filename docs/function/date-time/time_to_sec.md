# `TIME_TO_SEC`

## Description

Converts a time value to a number of seconds.

## Syntax

```sql
TIME_TO_SEC(time)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `time` | Yes | date, time, datetime, or the corresponding text/numeric value | Time or duration to process. |

## Return Value

Returns the signed number of seconds in a TIME/duration. A fractional input returns exact decimal seconds with the applicable precision. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT TIME_TO_SEC('01:01:01');
```

Result: 3661

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`MAKEDATE`](makedate.md), [`MAKETIME`](maketime.md), [`SEC_TO_TIME`](sec_to_time.md).

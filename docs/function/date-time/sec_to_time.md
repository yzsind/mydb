# `SEC_TO_TIME`

## Description

Converts a number of seconds to a time value.

## Syntax

```sql
SEC_TO_TIME(seconds)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `seconds` | Yes | Numeric expression | Signed number of seconds, including a fractional part. |

## Return Value

Returns a signed TIME from seconds, retaining applicable microsecond precision. Values outside the TIME range are clamped with a temporal warning. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT SEC_TO_TIME(3661);
```

Result: 01:01:01

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`MAKEDATE`](makedate.md), [`MAKETIME`](maketime.md), [`TIME_TO_SEC`](time_to_sec.md).

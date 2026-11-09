# `CURTIME`

## Description

Returns the current time in the session time zone.

## Syntax

```sql
CURTIME([fsp])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `fsp` | No | Integer from 0 to 6 | Fractional-second digits, 0 through 6; defaults to 0. |

## Return Value

Returns a TIME for the statement-start instant in the session time zone. fsp defaults to 0 and must be 0 through 6.

## Examples

```sql
SELECT CURTIME(3);
```

Result: Returns the current session-zone TIME with three fractional digits, for example `12:34:56.789`; the clock value changes between statements.

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`CURRENT_TIME`](current_time.md).

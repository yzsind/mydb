# `UTC_TIME`

## Description

Returns the current UTC time.

## Syntax

```sql
UTC_TIME([fsp])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `fsp` | No | Integer from 0 to 6 | Fractional-second digits, 0 through 6; defaults to 0. |

## Return Value

Returns a TIME for the statement-start instant in UTC. fsp defaults to 0 and must be 0 through 6.

## Examples

```sql
SELECT UTC_TIME(3);
```

Result: Returns the current UTC TIME with the requested three fractional digits, for example `04:34:56.789`; it changes with the clock.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`CURDATE`](curdate.md), [`CURTIME`](curtime.md), [`NOW`](now.md), [`SYSDATE`](sysdate.md).

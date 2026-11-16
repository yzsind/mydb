# `NOW`

## Description

Returns the current datetime in the session time zone, with optional fractional-seconds precision.

## Syntax

```sql
NOW([fsp])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `fsp` | No | Integer from 0 to 6 | Fractional-second digits, 0 through 6; defaults to 0. |

## Return Value

Returns a DATETIME for the statement-start instant in the session time zone. All calls within the same statement use that instant. fsp defaults to 0 and must be 0 through 6.

## Examples

```sql
SELECT NOW(3);
```

Result: returns the current session time with 3 fractional-seconds digits; the exact value changes with each execution.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`CURRENT_TIMESTAMP`](current_timestamp.md), [`LOCALTIME`](localtime.md), and [`LOCALTIMESTAMP`](localtimestamp.md).

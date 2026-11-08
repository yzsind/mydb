# `POW`

## Description

Returns the base raised to the specified power.

## Syntax

```sql
POW(base, exponent)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `base` | Yes | Numeric expression | Base of the power. |
| `exponent` | Yes | Numeric expression | Exponent of the power. |

## Return Value

Returns a DOUBLE. SQL NULL input returns NULL; a non-finite power result raises 1690/22003 instead of returning infinity or NaN.

## Examples

```sql
SELECT POW(2, 10);
```

Result: 1024

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`POWER`](power.md).

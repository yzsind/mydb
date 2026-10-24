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
| `base` | Yes | Numeric or integer | The 1st argument; the base of the power operation |
| `exponent` | Yes | Numeric or integer | The 2nd argument; the exponent of the power operation |

## Return Value

Returns a DOUBLE. SQL NULL input returns NULL; a non-finite power result raises 1690/22003 instead of returning infinity or NaN.

## Examples

```sql
SELECT POW(2, 10);
```

Result: 1024

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

[`POWER`](power.md).

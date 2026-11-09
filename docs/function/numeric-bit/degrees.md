# `DEGREES`

## Description

Converts radians to degrees.

## Syntax

```sql
DEGREES(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the angle in degrees as DOUBLE. SQL NULL returns NULL. A non-finite overflow raises error 1690/22003.

## Examples

```sql
SELECT DEGREES(2);
```

Result:

```text
114.59155902616465
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`SIN`](sin.md), [`COS`](cos.md), [`TAN`](tan.md), [`COT`](cot.md).

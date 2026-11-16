# `ATAN`

## Description

Returns the arc tangent; also supports two-argument quadrant computation.

## Syntax

```sql
ATAN(x)
ATAN(y, x)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `x_or_y` | Yes | Numeric | x in the one-argument form; y in the two-argument form. |
| `x` | Only with two arguments | Numeric | Horizontal coordinate for atan2(y,x). |

## Return Value

Returns a DOUBLE angle in radians. The one-argument form is arctangent; the two-argument form uses atan2(y,x) to select the quadrant. Any SQL NULL argument returns NULL.

## Examples

```sql
SELECT ATAN(1);
```

Result: approximately 0.7853981633974483

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`SIN`](sin.md), [`COS`](cos.md), [`TAN`](tan.md), [`COT`](cot.md).

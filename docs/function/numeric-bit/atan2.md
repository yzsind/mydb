# `ATAN2`

## Description

Returns the quadrant-aware arc tangent from two coordinate arguments.

## Syntax

```sql
ATAN2(y, x)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `y` | Yes | Numeric expression | Second numeric operand. |
| `x` | Yes | Numeric expression | First numeric operand. |

## Return Value

Returns the DOUBLE angle atan2(y,x), in radians, using both signs to select the quadrant. Any SQL NULL operand returns NULL.

## Examples

```sql
SELECT ATAN2(4, 2);
```

Result:

```text
1.1071487177940904
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`SIN`](sin.md), [`COS`](cos.md), [`TAN`](tan.md), [`COT`](cot.md).

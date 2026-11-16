# `ROUND`

## Description

Rounds a number to the specified number of decimal places.

## Syntax

```sql
ROUND(number [, decimals])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |
| `decimals` | No | Numeric expression | Decimal places; a negative value operates to the left of the decimal point. |

## Return Value

Returns the input rounded to decimals places, default 0. Exact DECIMAL half values round away from zero. Negative decimals round to tens, hundreds, and so on. SQL NULL in either argument returns NULL; the derived type/scale follows the numeric input domain.

## Examples

```sql
SELECT ROUND(12.55,1), ROUND(-12.55,1), ROUND(125,-1);
```

Result:

```text
12.6	-12.6	130
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`ABS`](abs.md), [`SIGN`](sign.md), [`CEIL`](ceil.md), [`FLOOR`](floor.md).

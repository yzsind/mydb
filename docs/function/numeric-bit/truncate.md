# `TRUNCATE`

## Description

Truncates a number to the specified number of decimal places.

## Syntax

```sql
TRUNCATE(number, decimals)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |
| `decimals` | Yes | Numeric expression | Decimal places; a negative value operates to the left of the decimal point. |

## Return Value

Returns the number with digits discarded toward zero at the requested decimal position. Negative decimals discard digits to the left of the decimal point. SQL NULL returns NULL; this operation does not round a half value upward.

## Examples

```sql
SELECT TRUNCATE(12.59,1), TRUNCATE(-12.59,1), TRUNCATE(129,-1);
```

Result:

```text
12.5	-12.5	120
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`ABS`](abs.md), [`SIGN`](sign.md), [`CEIL`](ceil.md), [`FLOOR`](floor.md).

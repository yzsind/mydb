# `ABS`

## Description

Returns the absolute value of a number.

## Syntax

```sql
ABS(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the absolute value in the derived integer, DECIMAL, or floating domain. SQL NULL returns NULL. Taking the absolute value of the smallest signed BIGINT raises numeric overflow.

## Examples

```sql
SELECT ABS(-12);
```

Result: 12

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`SIGN`](sign.md), [`CEIL`](ceil.md), [`FLOOR`](floor.md), [`ROUND`](round.md).

# `FLOOR`

## Description

Returns the largest integer not greater than the argument.

## Syntax

```sql
FLOOR(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the largest integral value less than or equal to the input. The derived exact/approximate numeric domain is retained; SQL NULL returns NULL.

## Examples

```sql
SELECT FLOOR(1.2), FLOOR(-1.2);
```

Result:

```text
1	-2
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`ABS`](abs.md), [`SIGN`](sign.md), [`CEIL`](ceil.md), [`ROUND`](round.md).

# `SIGN`

## Description

Returns the sign of a number.

## Syntax

```sql
SIGN(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns integer -1, 0, or 1 for a negative, zero, or positive converted number. SQL NULL returns NULL.

## Examples

```sql
SELECT SIGN(-2), SIGN(0), SIGN(2), SIGN(NULL);
```

Result:

```text
-1	0	1	NULL
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`ABS`](abs.md), [`CEIL`](ceil.md), [`FLOOR`](floor.md), [`ROUND`](round.md).

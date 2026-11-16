# `SQRT`

## Description

Returns the square root of a non-negative number.

## Syntax

```sql
SQRT(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns a DOUBLE square root. SQL NULL or a negative input returns SQL NULL.

## Examples

```sql
SELECT SQRT(2);
```

Result:

```text
1.4142135623730951
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[Category reference](README.md).

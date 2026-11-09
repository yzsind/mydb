# `DIV`

## Description

Performs integer division.

## Syntax

```sql
DIV(dividend, divisor)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `dividend` | Yes | Numeric | The dividend |
| `divisor` | Yes | Non-zero numeric | The divisor |

## Return Value

Returns the integer quotient truncated toward zero; the corresponding operator spelling is `dividend DIV divisor`. SQL NULL or zero divisor returns NULL, with the applicable division diagnostic. An out-of-range quotient raises a numeric error.

## Examples

```sql
SELECT -7 DIV 3, DIV(7,3);
```

Result:

```text
-2	2
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`MOD`](mod.md).

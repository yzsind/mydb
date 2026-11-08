# `MOD`

## Description

Returns the remainder of a division.

## Syntax

```sql
MOD(dividend, divisor)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `dividend` | Yes | Numeric expression | Number to divide. |
| `divisor` | Yes | Numeric expression | Number to divide by; zero produces the division-by-zero diagnostic. |

## Return Value

Returns the remainder in the common numeric domain. Its sign follows the dividend. SQL NULL or a zero divisor returns NULL, with the applicable division diagnostic.

## Examples

```sql
SELECT MOD(-7,3), MOD(7,-3);
```

Result:

```text
-1	1
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`DIV`](div.md).

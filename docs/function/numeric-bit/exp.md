# `EXP`

## Description

Returns e raised to the specified power.

## Syntax

```sql
EXP(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the exponential e raised to the operand as DOUBLE. SQL NULL returns NULL. A non-finite overflow raises error 1690/22003.

## Examples

```sql
SELECT EXP(2);
```

Result:

```text
7.38905609893065
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`LN`](ln.md), [`LOG`](log.md), [`LOG2`](log2.md), [`LOG10`](log10.md).

# `LN`

## Description

Returns the natural logarithm.

## Syntax

```sql
LN(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the natural logarithm as DOUBLE. SQL NULL returns NULL. Nonpositive input returns NULL with warning 3020 in SELECT; strict write contexts can reject it.

## Examples

```sql
SELECT LN(2);
```

Result:

```text
0.6931471805599453
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`LOG`](log.md), [`LOG2`](log2.md), [`LOG10`](log10.md), [`EXP`](exp.md).

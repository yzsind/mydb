# `LOG10`

## Description

Returns the base-10 logarithm.

## Syntax

```sql
LOG10(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the base-10 logarithm as DOUBLE. SQL NULL returns NULL. Nonpositive input returns NULL with warning 3020 in SELECT; strict write contexts can reject it.

## Examples

```sql
SELECT LOG10(2);
```

Result:

```text
0.3010299956639812
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`LN`](ln.md), [`LOG`](log.md), [`LOG2`](log2.md), [`EXP`](exp.md).

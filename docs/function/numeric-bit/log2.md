# `LOG2`

## Description

Returns the base-2 logarithm.

## Syntax

```sql
LOG2(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the base-2 logarithm as DOUBLE. SQL NULL returns NULL. Nonpositive input returns NULL with warning 3020 in SELECT; strict write contexts can reject it.

## Examples

```sql
SELECT LOG2(2);
```

Result:

```text
1
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`LN`](ln.md), [`LOG`](log.md), [`LOG10`](log10.md), [`EXP`](exp.md).

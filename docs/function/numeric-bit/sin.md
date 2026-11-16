# `SIN`

## Description

Returns the sine.

## Syntax

```sql
SIN(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the sine as DOUBLE. SQL NULL returns NULL. See the category reference for numeric conversion and exceptional domains.

## Examples

```sql
SELECT SIN(2);
```

Result:

```text
0.9092974268256817
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`COS`](cos.md), [`TAN`](tan.md), [`COT`](cot.md), [`ASIN`](asin.md).

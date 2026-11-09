# `TAN`

## Description

Returns the tangent.

## Syntax

```sql
TAN(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the tangent as DOUBLE. SQL NULL returns NULL. See the category reference for numeric conversion and exceptional domains.

## Examples

```sql
SELECT TAN(2);
```

Result:

```text
-2.185039863261519
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`SIN`](sin.md), [`COS`](cos.md), [`COT`](cot.md), [`ASIN`](asin.md).

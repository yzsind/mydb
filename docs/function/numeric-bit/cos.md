# `COS`

## Description

Returns the cosine.

## Syntax

```sql
COS(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the cosine as DOUBLE. SQL NULL returns NULL. See the category reference for numeric conversion and exceptional domains.

## Examples

```sql
SELECT COS(2);
```

Result:

```text
-0.4161468365471424
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`SIN`](sin.md), [`TAN`](tan.md), [`COT`](cot.md), [`ASIN`](asin.md).

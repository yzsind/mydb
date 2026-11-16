# `COT`

## Description

Returns the cotangent.

## Syntax

```sql
COT(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the cotangent as DOUBLE. SQL NULL returns NULL. A non-finite overflow raises error 1690/22003.

## Examples

```sql
SELECT COT(2);
```

Result:

```text
-0.45765755436028577
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`SIN`](sin.md), [`COS`](cos.md), [`TAN`](tan.md), [`ASIN`](asin.md).

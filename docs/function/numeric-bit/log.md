# `LOG`

## Description

Returns the natural logarithm, or the logarithm to a specified base.

## Syntax

```sql
LOG(x)
LOG(base, x)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `base_or_x` | Yes | Positive number | The antilogarithm in the single-argument form; the base in the two-argument form |
| `x` | No | Positive number | The antilogarithm in the two-argument form |

## Return Value

Returns a DOUBLE logarithm: natural logarithm with one argument, or logarithm of x in the supplied base with two. SQL NULL input returns NULL; x must be positive and the base must be positive and different from 1. Invalid domains return NULL with the applicable diagnostic.

## Examples

```sql
SELECT LOG(10);
```

Result: approximately 2.302585092994046

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`LN`](ln.md), [`LOG2`](log2.md), [`LOG10`](log10.md), [`EXP`](exp.md).

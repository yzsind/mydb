# `ASIN`

## Description

Returns the arc sine.

## Syntax

```sql
ASIN(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the arc sine in [-pi/2,pi/2] radians as DOUBLE. SQL NULL returns NULL. Inputs outside [-1,1] return NULL.

## Examples

```sql
SELECT ASIN(0);
```

Result:

```text
0
```

## Notes

- The real-valued input domain is [-1,1]. An out-of-domain value such as 2 returns NULL with the applicable numeric diagnostic; it is not a successful trigonometric result.

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`SIN`](sin.md), [`COS`](cos.md), [`TAN`](tan.md), [`COT`](cot.md).

# `CEIL`

## Description

Returns the smallest integer not less than the argument.

## Syntax

```sql
CEIL(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the smallest integral value greater than or equal to the input. The derived exact/approximate numeric domain is retained; SQL NULL returns NULL.

## Examples

```sql
SELECT CEIL(1.2), CEIL(-1.2);
```

Result:

```text
2	-1
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`CEILING`](ceiling.md).

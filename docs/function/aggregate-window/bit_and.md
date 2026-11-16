# `BIT_AND`

## Description

Performs a bitwise AND aggregation over non-`NULL` values in the group.

## Syntax

```sql
BIT_AND(expr)
BIT_AND(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | Expression to evaluate. |

## Return Value

Returns the bitwise AND of non-NULL inputs. Numeric evaluation is unsigned 64-bit; empty or all-NULL numeric input returns 18446744073709551615 (all bits set). Binary-string evaluation uses equal-length operands and an all-ones neutral value of that length. OVER applies the same rule within a frame.

## Examples

```sql
SELECT BIT_AND(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
0
```

```sql
SELECT BIT_AND(v) FROM (SELECT NULL v WHERE FALSE) AS empty_input;
SELECT BIT_AND(v) FROM (SELECT NULL v) AS one_null;
```

Results: 18446744073709551615, then 18446744073709551615.

## Notes

- Hex/bit literals select numeric evaluation unless an explicit binary type establishes byte semantics. Use CAST(... AS BINARY(n)) or a typed binary column when the aggregate must operate on bytes; operands must have equal byte lengths.

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

- Binary evaluation requires equal byte lengths; incompatible lengths raise a bitwise-operand error. Numeric empty/all-NULL neutral values differ from typed-binary neutral bytes. Explicitly establish a binary input domain when bytewise aggregation is intended.

## Related Functions

[`BIT_OR`](bit_or.md), [`BIT_XOR`](bit_xor.md).

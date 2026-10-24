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
| `expr` | Yes | Any SQL expression | The 1st argument; the expression to evaluate |

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

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

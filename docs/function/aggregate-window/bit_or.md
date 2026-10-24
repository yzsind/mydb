# `BIT_OR`

## Description

Performs a bitwise OR aggregation over non-`NULL` values in the group.

## Syntax

```sql
BIT_OR(expr)
BIT_OR(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | The 1st argument; the expression to evaluate |

## Return Value

Returns the bitwise OR of non-NULL inputs. Numeric evaluation is unsigned 64-bit; empty or all-NULL numeric input returns 0. Binary-string evaluation uses equal-length operands and a zero-filled neutral value of that length. OVER applies the same rule within a frame.

## Examples

```sql
SELECT BIT_OR(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
3
```

```sql
SELECT BIT_OR(v) FROM (SELECT NULL v WHERE FALSE) AS empty_input;
SELECT BIT_OR(v) FROM (SELECT NULL v) AS one_null;
```

Results: 0, then 0.

## Notes

- Hex/bit literals select numeric evaluation unless an explicit binary type establishes byte semantics. Use CAST(... AS BINARY(n)) or a typed binary column when the aggregate must operate on bytes; operands must have equal byte lengths.

- For window-capable functions, OVER controls the partition and ordering; frame-sensitive aggregates and value functions use the selected frame. Ranking and LAG/LEAD use partition positions. The function-specific NULL and empty-input rules above apply.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

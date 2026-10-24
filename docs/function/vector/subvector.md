# `SUBVECTOR`

## Description

Extracts a subvector by start position and length.

## Syntax

```sql
SUBVECTOR(vector, start, count)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `vector` | Yes | VECTOR or valid vector text | A nonempty native vector; integer scalars and metric names are not vector values |
| `start` | Yes | Integer or integral numeric string | One-based first element; must be at least 1 |
| `count` | Yes | Integer or integral numeric string | Positive element count; the full slice must fit |

## Return Value

Returns a VECTOR with the input datatype; returns `NULL` when any argument is `NULL`.

## Examples

```sql
SELECT SUBVECTOR(STRING_TO_VECTOR('[1,2]'), 1, 2);
```

Result:

```text
[1,2]
```

## Notes

- BINARY start/count are measured in bits; the slice is repacked with zero padding.
- Indexing starts at 1. Both start and count must be positive, and `start + count - 1` must not exceed the dimension. The function rejects an invalid slice rather than clipping it or returning an empty vector.
- For example, `SUBVECTOR('[10,20,30]',2,2)` returns `[20,30]`. Floating-point start/count values are not integral index arguments.

- Vectors must be nonempty, floating elements must be finite, and distance operands must have equal dimensions. Numeric datatypes may be mixed without narrowing the query. BINARY cannot be mixed with numeric vectors. Invalid encodings and scalar integer operands raise a vector error. Raw payloads require `VECTOR_FROM_BINARY`; metric names are case-insensitive.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

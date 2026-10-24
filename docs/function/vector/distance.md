# `DISTANCE`

## Description

Computes the distance between two vectors using the specified metric, defaulting to L2 for numeric vectors and HAMMING for BINARY.

## Syntax

```sql
DISTANCE(vector1, vector2 [, metric])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `vector1` | Yes | VECTOR or valid vector text | The first vector |
| `vector2` | Yes | VECTOR or valid vector text | The second vector, of the same dimension |
| `metric` | No | L2, COSINE, DOT, HAMMING, or JACCARD | Defaults to L2 for numeric vectors, HAMMING for BINARY |

## Return Value

Returns a double-precision value; returns `NULL` when any argument is `NULL`.

## Examples

```sql
SELECT DISTANCE(STRING_TO_VECTOR('[1,2]'), STRING_TO_VECTOR('[1,2]'));
```

Result:

```text
0
```

## Notes

- L2 aliases are `L2`, `EUCLIDEAN`, `EUCLIDEAN_DISTANCE`; cosine aliases are `COSINE`, `COS`; inner-product aliases are `DOT`, `IP`, `INNER_PRODUCT`, `DOT_PRODUCT`, `NEGATIVE_INNER_PRODUCT`.
- L2 returns Euclidean distance; COSINE returns `1 - cosine_similarity` and returns SQL NULL if either vector has zero norm. DOT returns the negative inner product, so ascending distance orders the largest inner products first. `DOT_PRODUCT(a,b)` instead returns the positive inner product.
- `EUCLIDEAN_DISTANCE` is a metric spelling, not a scalar function. For example, `DISTANCE('[0,0]','[3,4]','EUCLIDEAN_DISTANCE')` returns 5.

- Vectors must be nonempty, floating elements must be finite, and distance operands must have equal dimensions. Numeric datatypes may be mixed without narrowing the query. BINARY cannot be mixed with numeric vectors. Invalid encodings and scalar integer operands raise a vector error. Raw payloads require `VECTOR_FROM_BINARY`; metric names are case-insensitive.

HAMMING counts XOR bits. JACCARD is `1 - popcount(a AND b)/popcount(a OR b)` and returns zero for two all-zero vectors. These metrics require BINARY operands.

## Related Functions

[`VEC_DISTANCE`](vec_distance.md): the alias of this function.

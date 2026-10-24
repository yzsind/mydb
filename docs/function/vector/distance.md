# `DISTANCE`

## Description

Computes the distance between two vectors using the specified metric, defaulting to L2.

## Syntax

```sql
DISTANCE(vector1, vector2 [, metric])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `vector1` | Yes | VECTOR, valid vector text, or encoded vector bytes | The first vector |
| `vector2` | Yes | VECTOR, valid vector text, or encoded vector bytes | The second vector, of the same dimension |
| `metric` | No | L2, COSINE, or DOT | Defaults to L2 |

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

- Vector values must be nonempty, contain finite f32 elements, and binary vector operations require equal dimensions. Invalid encodings, dimensions or scalar integer operands return an explicit vector error. Distance metric names are case-insensitive.

## Related Functions

[`VEC_DISTANCE`](vec_distance.md): the alias of this function.

# `L2_DISTANCE`

## Description

Computes the Euclidean distance between two vectors.

## Syntax

```sql
L2_DISTANCE(vector1, vector2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `vector1` | Yes | VECTOR or valid vector text | The first vector |
| `vector2` | Yes | VECTOR or valid vector text | The second vector |

## Return Value

Returns a double-precision value; returns `NULL` when any argument is `NULL`.

## Examples

```sql
SELECT L2_DISTANCE(STRING_TO_VECTOR('[0,0]'), STRING_TO_VECTOR('[3,4]'));
```

Result: 5

## Notes

- Vectors must be nonempty, floating elements must be finite, and distance operands must have equal dimensions. Numeric datatypes may be mixed without narrowing the query. BINARY cannot be mixed with numeric vectors. Invalid encodings and scalar integer operands raise a vector error. Raw payloads require `VECTOR_FROM_BINARY`; metric names are case-insensitive.

## Related Functions

[`DISTANCE`](distance.md), [`COSINE_DISTANCE`](cosine_distance.md), [`DOT_PRODUCT`](dot_product.md), [`MAX_INNER_PRODUCT_DISTANCE`](max_inner_product_distance.md).

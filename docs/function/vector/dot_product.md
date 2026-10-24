# `DOT_PRODUCT`

## Description

Computes the dot product of two vectors.

## Syntax

```sql
DOT_PRODUCT(vector1, vector2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `vector1` | Yes | VECTOR or valid vector text | The 1st argument; the first vector |
| `vector2` | Yes | VECTOR or valid vector text | The 2nd argument; the second vector |

## Return Value

Returns a double-precision value; returns `NULL` when any argument is `NULL`.

## Examples

```sql
SELECT DOT_PRODUCT(STRING_TO_VECTOR('[1,2]'), STRING_TO_VECTOR('[1,2]'));
```

Result:

```text
5
```

## Notes

- Returns the positive sum of elementwise products. `DISTANCE(a,b,'DOT')` and `MAX_INNER_PRODUCT_DISTANCE(a,b)` return its negation for ascending nearest-neighbor ordering.

- Vectors must be nonempty, floating elements must be finite, and distance operands must have equal dimensions. Numeric datatypes may be mixed without narrowing the query. BINARY cannot be mixed with numeric vectors. Invalid encodings and scalar integer operands raise a vector error. Raw payloads require `VECTOR_FROM_BINARY`; metric names are case-insensitive.

## Related Functions

[`INNER_PRODUCT`](inner_product.md): the alias of this function.

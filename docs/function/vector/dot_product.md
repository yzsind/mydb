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
| `vector1` | Yes | VECTOR, valid vector text, or encoded vector bytes | The 1st argument; the first vector |
| `vector2` | Yes | VECTOR, valid vector text, or encoded vector bytes | The 2nd argument; the second vector |

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

- Vector values must be nonempty, contain finite f32 elements, and binary vector operations require equal dimensions. Invalid encodings, dimensions or scalar integer operands return an explicit vector error. Distance metric names are case-insensitive.

## Related Functions

[`INNER_PRODUCT`](inner_product.md): the alias of this function.

# `MAX_INNER_PRODUCT_DISTANCE`

## Description

Returns the negation of the inner product, used for maximum inner product nearest-neighbor ordering.

## Syntax

```sql
MAX_INNER_PRODUCT_DISTANCE(vector1, vector2)
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
SELECT MAX_INNER_PRODUCT_DISTANCE(STRING_TO_VECTOR('[1,2]'), STRING_TO_VECTOR('[1,2]'));
```

Result:

```text
-5
```

## Notes

- Vector values must be nonempty, contain finite f32 elements, and binary vector operations require equal dimensions. Invalid encodings, dimensions or scalar integer operands return an explicit vector error. Distance metric names are case-insensitive.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

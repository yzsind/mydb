# `VECTOR_DIM`

## Description

Returns the dimension of a vector.

## Syntax

```sql
VECTOR_DIM(vector)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `vector` | Yes | VECTOR, valid vector text, or encoded vector bytes | A nonempty finite-f32 vector; integer scalars and metric names are not vector values |

## Return Value

Returns the dimension as an integer; returns `NULL` when the argument is `NULL`.

## Examples

```sql
SELECT VECTOR_DIM(STRING_TO_VECTOR('[1,2,3]'));
```

Result: 3

## Notes

- Vector values must be nonempty, contain finite f32 elements, and binary vector operations require equal dimensions. Invalid encodings, dimensions or scalar integer operands return an explicit vector error. Distance metric names are case-insensitive.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

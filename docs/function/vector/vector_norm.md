# `VECTOR_NORM`

## Description

Returns the L2 norm of a vector.

## Syntax

```sql
VECTOR_NORM(vector)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `vector` | Yes | VECTOR, valid vector text, or encoded vector bytes | A nonempty finite-f32 vector; integer scalars and metric names are not vector values |

## Return Value

Returns a double-precision value; returns `NULL` when any argument is `NULL`.

## Examples

```sql
SELECT VECTOR_NORM(STRING_TO_VECTOR('[1,2]'));
```

Result:

```text
2.23606797749979
```

## Notes

- Vector values must be nonempty, contain finite f32 elements, and binary vector operations require equal dimensions. Invalid encodings, dimensions or scalar integer operands return an explicit vector error. Distance metric names are case-insensitive.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

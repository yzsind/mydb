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
| `vector` | Yes | VECTOR or valid vector text | A nonempty native vector; integer scalars and metric names are not vector values |

## Return Value

Returns the element count (bit count for BINARY) as an integer; returns `NULL` when the argument is `NULL`.

## Examples

```sql
SELECT VECTOR_DIM(STRING_TO_VECTOR('[1,2,3]'));
```

Result: 3

## Notes

- Vectors must be nonempty, floating elements must be finite, and distance operands must have equal dimensions. Numeric datatypes may be mixed without narrowing the query. BINARY cannot be mixed with numeric vectors. Invalid encodings and scalar integer operands raise a vector error. Raw payloads require `VECTOR_FROM_BINARY`; metric names are case-insensitive.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

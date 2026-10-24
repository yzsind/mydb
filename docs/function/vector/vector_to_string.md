# `VECTOR_TO_STRING`

## Description

Converts a vector to its text representation.

## Syntax

```sql
VECTOR_TO_STRING(vector)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `vector` | Yes | VECTOR, valid vector text, or encoded vector bytes | A nonempty finite-f32 vector; integer scalars and metric names are not vector values |

## Return Value

Returns the vector text representation; returns `NULL` when the argument is `NULL`.

## Examples

```sql
SELECT VECTOR_TO_STRING(STRING_TO_VECTOR('[1,2]'));
```

Result: [1,2]

## Notes

- Vector values must be nonempty, contain finite f32 elements, and binary vector operations require equal dimensions. Invalid encodings, dimensions or scalar integer operands return an explicit vector error. Distance metric names are case-insensitive.

## Related Functions

[`FROM_VECTOR`](from_vector.md): the alias of this function.

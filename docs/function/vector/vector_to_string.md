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
| `vector` | Yes | VECTOR or valid vector text | A nonempty native vector; integer scalars and metric names are not vector values |

## Return Value

Returns the vector text representation; returns `NULL` when the argument is `NULL`.

## Examples

```sql
SELECT VECTOR_TO_STRING(STRING_TO_VECTOR('[1,2]'));
```

Result: [1,2]

## Notes

- Vectors must be nonempty, floating elements must be finite, and distance operands must have equal dimensions. Numeric datatypes may be mixed without narrowing the query. BINARY cannot be mixed with numeric vectors. Invalid encodings and scalar integer operands raise a vector error. Raw payloads require `VECTOR_FROM_BINARY`; metric names are case-insensitive.

## Related Functions

[`FROM_VECTOR`](from_vector.md): the alias of this function.

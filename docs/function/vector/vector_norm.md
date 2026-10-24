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
| `vector` | Yes | VECTOR or valid vector text | A nonempty native vector; integer scalars and metric names are not vector values |

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

- Supports FLOAT32, FLOAT16, BFLOAT16 and INT8. BINARY raises an error.

- Vectors must be nonempty, floating elements must be finite, and distance operands must have equal dimensions. Numeric datatypes may be mixed without narrowing the query. BINARY cannot be mixed with numeric vectors. Invalid encodings and scalar integer operands raise a vector error. Raw payloads require `VECTOR_FROM_BINARY`; metric names are case-insensitive.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

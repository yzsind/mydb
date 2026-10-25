# `COSINE_DISTANCE`

## Description

Computes the cosine distance between two vectors.

## Syntax

```sql
COSINE_DISTANCE(vector1, vector2)
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
SELECT COSINE_DISTANCE(STRING_TO_VECTOR('[1,2]'), STRING_TO_VECTOR('[1,2]'));
```

Result:

```text
0
```

## Notes

- Returns `1 - dot(a,b)/(norm(a)*norm(b))`. If either vector has zero norm, returns SQL NULL. For example, `COSINE_DISTANCE('[0,0]','[1,2]')` returns NULL.

- Vectors must be nonempty, floating elements must be finite, and distance operands must have equal dimensions. Numeric datatypes may be mixed without narrowing the query. BINARY cannot be mixed with numeric vectors. Invalid encodings and scalar integer operands raise a vector error. Raw payloads require `VECTOR_FROM_BINARY`; metric names are case-insensitive.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

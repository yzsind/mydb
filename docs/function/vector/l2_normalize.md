# `L2_NORMALIZE`

## Description

Normalizes a vector to a unit vector.

## Syntax

```sql
L2_NORMALIZE(vector)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `vector` | Yes | VECTOR or valid vector text | A nonempty native vector; integer scalars and metric names are not vector values |

## Return Value

Returns a FLOAT32 VECTOR; returns `NULL` when any argument is `NULL`.

## Examples

```sql
SELECT L2_NORMALIZE(STRING_TO_VECTOR('[1,2]'));
```

Result:

```text
[0.4472136,0.8944272]
```

## Notes

- Supports numeric datatypes; BINARY raises an error. The result must fit the FLOAT32 dimension limit of 16383.
- A zero vector retains its components after conversion to FLOAT32; `L2_NORMALIZE('[0,0]')` returns `[0,0]`. Nonzero results retain f32 precision, so decimal rendering can show rounding.

- Vectors must be nonempty, floating elements must be finite, and distance operands must have equal dimensions. Numeric datatypes may be mixed without narrowing the query. BINARY cannot be mixed with numeric vectors. Invalid encodings and scalar integer operands raise a vector error. Raw payloads require `VECTOR_FROM_BINARY`; metric names are case-insensitive.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

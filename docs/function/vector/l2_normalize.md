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
| `vector` | Yes | VECTOR, valid vector text, or encoded vector bytes | A nonempty finite-f32 vector; integer scalars and metric names are not vector values |

## Return Value

Returns a VECTOR; returns `NULL` when any argument is `NULL`.

## Examples

```sql
SELECT L2_NORMALIZE(STRING_TO_VECTOR('[1,2]'));
```

Result:

```text
[0.4472136,0.8944272]
```

## Notes

- A zero vector is returned unchanged; `L2_NORMALIZE('[0,0]')` returns `[0,0]`. Nonzero results retain f32 precision, so decimal rendering can show rounding.

- Vector values must be nonempty, contain finite f32 elements, and binary vector operations require equal dimensions. Invalid encodings, dimensions or scalar integer operands return an explicit vector error. Distance metric names are case-insensitive.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

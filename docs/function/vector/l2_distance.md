# `L2_DISTANCE`

## Description

Computes the Euclidean distance between two vectors.

## Syntax

```sql
L2_DISTANCE(vector1, vector2)
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
SELECT L2_DISTANCE(STRING_TO_VECTOR('[0,0]'), STRING_TO_VECTOR('[3,4]'));
```

Result: 5

## Notes

- Vector values must be nonempty, contain finite f32 elements, and binary vector operations require equal dimensions. Invalid encodings, dimensions or scalar integer operands return an explicit vector error. Distance metric names are case-insensitive.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

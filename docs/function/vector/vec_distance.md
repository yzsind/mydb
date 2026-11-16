# `VEC_DISTANCE`

## Description

Computes the vector distance using the specified metric; an alias of `DISTANCE`. This function shares the same implementation and semantics as [`DISTANCE`](distance.md).

## Syntax

```sql
VEC_DISTANCE(vector1, vector2 [, metric])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`DISTANCE`](distance.md); refer to the primary function's documentation.

## Examples

```sql
SELECT VEC_DISTANCE(STRING_TO_VECTOR('[1,2]'), STRING_TO_VECTOR('[1,2]'));
```

Result:

```text
0
```

## Related Functions

- [`DISTANCE`](distance.md): the primary function with full documentation.

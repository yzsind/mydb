# `TO_VECTOR`

## Description

Parses a text representation into a vector; an alias of `STRING_TO_VECTOR`. This function shares the same implementation and semantics as [`STRING_TO_VECTOR`](string_to_vector.md).

## Syntax

```sql
TO_VECTOR(vector)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`STRING_TO_VECTOR`](string_to_vector.md); refer to the primary function's documentation.

## Examples

```sql
SELECT VECTOR_DIM(TO_VECTOR('[1,2,3]'));
```

Result: 3

## Related Functions

- [`STRING_TO_VECTOR`](string_to_vector.md): the primary function with full documentation.

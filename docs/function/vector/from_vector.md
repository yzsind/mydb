# `FROM_VECTOR`

## Description

Converts a vector to its text representation; an alias of `VECTOR_TO_STRING`. This function shares the same implementation and semantics as [`VECTOR_TO_STRING`](vector_to_string.md).

## Syntax

```sql
FROM_VECTOR(vector)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`VECTOR_TO_STRING`](vector_to_string.md); refer to the primary function's documentation.

## Examples

```sql
SELECT FROM_VECTOR(STRING_TO_VECTOR('[1,2]'));
```

Result: [1,2]

## Related Functions

- [`VECTOR_TO_STRING`](vector_to_string.md): the primary function with full documentation.

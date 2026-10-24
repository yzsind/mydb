# `STRING_TO_VECTOR`

## Description

Parses a text representation into a vector.

## Syntax

```sql
STRING_TO_VECTOR(vector)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `vector` | Yes | Vector text, UTF-8 bytes, or VECTOR | Parses bracketed numeric text such as `[1,2,3]`; VECTOR inputs are rendered then parsed |

## Return Value

Returns a VECTOR; returns `NULL` when any argument is `NULL`.

## Examples

```sql
SELECT VECTOR_DIM(STRING_TO_VECTOR('[1,2,3]'));
```

Result: 3

## Notes

- Vector values must be nonempty, contain finite f32 elements, and binary vector operations require equal dimensions. Invalid encodings, dimensions or scalar integer operands return an explicit vector error. Distance metric names are case-insensitive.

## Related Functions

[`TO_VECTOR`](to_vector.md): the alias of this function.

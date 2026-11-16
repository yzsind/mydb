# `INNER_PRODUCT`

## Description

Computes the inner product of two vectors; an alias of `DOT_PRODUCT`. This function shares the same implementation and semantics as [`DOT_PRODUCT`](dot_product.md).

## Syntax

```sql
INNER_PRODUCT(vector1, vector2)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`DOT_PRODUCT`](dot_product.md); refer to the primary function's documentation.

## Examples

```sql
SELECT INNER_PRODUCT(STRING_TO_VECTOR('[1,2]'), STRING_TO_VECTOR('[1,2]'));
```

Result:

```text
5
```

## Related Functions

- [`DOT_PRODUCT`](dot_product.md): the primary function with full documentation.

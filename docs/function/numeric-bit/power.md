# `POWER`

## Description

Returns the base raised to the specified power; an alias of `POW`. This function shares the same implementation and semantics as [`POW`](pow.md).

## Syntax

```sql
POWER(base, exponent)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`POW`](pow.md); refer to the primary function's documentation.

## Examples

```sql
SELECT POWER(2, 10);
```

Result: 1024

## Related Functions

- [`POW`](pow.md): the primary function with full documentation.

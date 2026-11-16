# `CEILING`

## Description

Returns the smallest integer not less than the argument; an alias of `CEIL`. This function shares the same implementation and semantics as [`CEIL`](ceil.md).

## Syntax

```sql
CEILING(number)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`CEIL`](ceil.md); refer to the primary function's documentation.

## Examples

```sql
SELECT CEILING(1.2);
```

Result: 2

## Related Functions

- [`CEIL`](ceil.md): the primary function with full documentation.

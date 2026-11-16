# `OCTET_LENGTH`

## Description

Returns the length of a string in bytes; an alias of `LENGTH`. This function shares the same implementation and semantics as [`LENGTH`](length.md).

## Syntax

```sql
OCTET_LENGTH(str)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`LENGTH`](length.md); refer to the primary function's documentation.

## Examples

```sql
SELECT OCTET_LENGTH('mydb');
```

Result: 4

## Related Functions

- [`LENGTH`](length.md): the primary function with full documentation.

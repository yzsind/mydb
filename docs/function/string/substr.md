# `SUBSTR`

## Description

Extracts a substring; an alias of `SUBSTRING`. This function shares the same implementation and semantics as [`SUBSTRING`](substring.md).

## Syntax

```sql
SUBSTR(str, pos)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`SUBSTRING`](substring.md); refer to the primary function's documentation.

## Examples

```sql
SELECT SUBSTR('mydb', 3, 2);
```

Result: db

## Related Functions

- [`SUBSTRING`](substring.md): the primary function with full documentation.

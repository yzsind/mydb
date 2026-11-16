# `UCASE`

## Description

Converts a string to uppercase; an alias of `UPPER`. This function shares the same implementation and semantics as [`UPPER`](upper.md).

## Syntax

```sql
UCASE(str)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`UPPER`](upper.md); refer to the primary function's documentation.

## Examples

```sql
SELECT UCASE('mydb');
```

Result: MYDB

## Related Functions

- [`UPPER`](upper.md): the primary function with full documentation.

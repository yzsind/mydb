# `LCASE`

## Description

Converts a string to lowercase; an alias of `LOWER`. This function shares the same implementation and semantics as [`LOWER`](lower.md).

## Syntax

```sql
LCASE(str)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`LOWER`](lower.md); refer to the primary function's documentation.

## Examples

```sql
SELECT LCASE('MyDB');
```

Result: mydb

## Related Functions

- [`LOWER`](lower.md): the primary function with full documentation.

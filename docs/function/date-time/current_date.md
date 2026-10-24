# `CURRENT_DATE`

## Description

Returns the current date in the session time zone; an alias of `CURDATE`. This function shares the same implementation and semantics as [`CURDATE`](curdate.md).

## Syntax

```sql
CURRENT_DATE()
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`CURDATE`](curdate.md); refer to the primary function's documentation.

## Examples

```sql
SELECT CURRENT_DATE();
```

Result: returns the current session date; the exact value varies with the execution date.

## Related Functions

- [`CURDATE`](curdate.md): the primary function with full documentation.

# `LOCALTIME`

## Description

Returns the current date and time in the session time zone; an alias of `NOW`. This function shares the same implementation and semantics as [`NOW`](now.md).

## Syntax

```sql
LOCALTIME([fsp])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`NOW`](now.md); refer to the primary function's documentation.

## Examples

```sql
SELECT LOCALTIME(3);
```

Result: returns the current session time with 3 fractional-seconds digits; the exact value changes with each execution.

## Related Functions

- [`NOW`](now.md): the primary function with full documentation.

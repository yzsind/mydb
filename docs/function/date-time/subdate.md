# `SUBDATE`

## Description

Subtracts an interval from a date or datetime; an alias of `DATE_SUB`. This function shares the same implementation and semantics as [`DATE_SUB`](date_sub.md).

## Syntax

```sql
SUBDATE(date, INTERVAL expr unit)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`DATE_SUB`](date_sub.md); refer to the primary function's documentation.

## Examples

```sql
SELECT SUBDATE('2026-08-30', INTERVAL 1 DAY);
```

Result: 2026-08-29

## Related Functions

- [`DATE_SUB`](date_sub.md): the primary function with full documentation.

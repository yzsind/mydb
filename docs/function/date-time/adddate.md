# `ADDDATE`

## Description

Adds an interval to a date or datetime; an alias of `DATE_ADD`. This function shares the same implementation and semantics as [`DATE_ADD`](date_add.md).

## Syntax

```sql
ADDDATE(date, INTERVAL expr unit)
ADDDATE(date, days)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`DATE_ADD`](date_add.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ADDDATE('2026-08-30', INTERVAL 1 DAY);
```

Result: 2026-08-31

## Related Functions

- [`DATE_ADD`](date_add.md): the primary function with full documentation.

## Notes

- The two-argument numeric form interprets days as a signed day count. It is an additional form beyond the INTERVAL syntax of the primary function.

- The two-argument numeric form interprets days as a signed day count. It is an additional form beyond the INTERVAL syntax of the primary function.

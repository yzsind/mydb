# `ST_MULTIPOINTFROMTEXT`

## Description

Constructs a multipoint from WKT; an alias of `ST_MPOINTFROMTEXT`. This function shares the same implementation and semantics as [`ST_MPOINTFROMTEXT`](st_mpointfromtext.md).

## Syntax

```sql
ST_MULTIPOINTFROMTEXT(wkt [, srid] [, options])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_MPOINTFROMTEXT`](st_mpointfromtext.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(ST_MULTIPOINTFROMTEXT('MULTIPOINT((0 0),(1 1))'));
```

Result: `MULTIPOINT(0 0,1 1)`.

## Related Functions

- [`ST_MPOINTFROMTEXT`](st_mpointfromtext.md): the primary function with full documentation.

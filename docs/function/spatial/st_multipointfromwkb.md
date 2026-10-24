# `ST_MULTIPOINTFROMWKB`

## Description

Constructs a multipoint from WKB; an alias of `ST_MPOINTFROMWKB`. This function shares the same implementation and semantics as [`ST_MPOINTFROMWKB`](st_mpointfromwkb.md).

## Syntax

```sql
ST_MULTIPOINTFROMWKB(wkb [, srid] [, options])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_MPOINTFROMWKB`](st_mpointfromwkb.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(ST_MULTIPOINTFROMWKB(ST_ASBINARY(ST_GEOMFROMTEXT('MULTIPOINT((0 0),(1 1))'))));
```

Result: `MULTIPOINT(0 0,1 1)`.

## Related Functions

- [`ST_MPOINTFROMWKB`](st_mpointfromwkb.md): the primary function with full documentation.

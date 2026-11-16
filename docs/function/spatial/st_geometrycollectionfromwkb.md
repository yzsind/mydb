# `ST_GEOMETRYCOLLECTIONFROMWKB`

## Description

Constructs a geometry collection from WKB; an alias of `ST_GEOMCOLLFROMWKB`. This function shares the same implementation and semantics as [`ST_GEOMCOLLFROMWKB`](st_geomcollfromwkb.md).

## Syntax

```sql
ST_GEOMETRYCOLLECTIONFROMWKB(wkb [, srid] [, options])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_GEOMCOLLFROMWKB`](st_geomcollfromwkb.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(ST_GEOMETRYCOLLECTIONFROMWKB(ST_ASBINARY(ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(POINT(1 2))'))));
```

Result: `GEOMETRYCOLLECTION(POINT(1 2))`.

## Related Functions

- [`ST_GEOMCOLLFROMWKB`](st_geomcollfromwkb.md): the primary function with full documentation.

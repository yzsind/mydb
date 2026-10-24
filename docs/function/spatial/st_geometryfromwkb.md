# `ST_GEOMETRYFROMWKB`

## Description

Constructs a geometry from WKB; an alias of `ST_GEOMFROMWKB`. This function shares the same implementation and semantics as [`ST_GEOMFROMWKB`](st_geomfromwkb.md).

## Syntax

```sql
ST_GEOMETRYFROMWKB(wkb [, srid] [, options])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_GEOMFROMWKB`](st_geomfromwkb.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(ST_GEOMETRYFROMWKB(ST_ASBINARY(ST_GEOMFROMTEXT('POINT(1 2)'))));
```

Result: `POINT(1 2)`.

## Related Functions

- [`ST_GEOMFROMWKB`](st_geomfromwkb.md): the primary function with full documentation.

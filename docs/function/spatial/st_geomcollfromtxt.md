# `ST_GEOMCOLLFROMTXT`

## Description

Constructs a geometry collection from WKT; an alias of `ST_GEOMCOLLFROMTEXT`. This function shares the same implementation and semantics as [`ST_GEOMCOLLFROMTEXT`](st_geomcollfromtext.md).

## Syntax

```sql
ST_GEOMCOLLFROMTXT(wkt [, srid] [, options])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_GEOMCOLLFROMTEXT`](st_geomcollfromtext.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(ST_GEOMCOLLFROMTXT('GEOMETRYCOLLECTION(POINT(1 2))'));
```

Result: `GEOMETRYCOLLECTION(POINT(1 2))`.

## Related Functions

- [`ST_GEOMCOLLFROMTEXT`](st_geomcollfromtext.md): the primary function with full documentation.

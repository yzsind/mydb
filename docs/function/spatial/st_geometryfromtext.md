# `ST_GEOMETRYFROMTEXT`

## Description

Constructs a geometry from WKT; an alias of `ST_GEOMFROMTEXT`. This function shares the same implementation and semantics as [`ST_GEOMFROMTEXT`](st_geomfromtext.md).

## Syntax

```sql
ST_GEOMETRYFROMTEXT(wkt [, srid] [, options])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_GEOMFROMTEXT`](st_geomfromtext.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(ST_GEOMETRYFROMTEXT('POINT(1 2)'));
```

Result: `POINT(1 2)`.

## Related Functions

- [`ST_GEOMFROMTEXT`](st_geomfromtext.md): the primary function with full documentation.

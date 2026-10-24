# `ST_POLYGONFROMWKB`

## Description

Constructs a polygon from WKB; an alias of `ST_POLYFROMWKB`. This function shares the same implementation and semantics as [`ST_POLYFROMWKB`](st_polyfromwkb.md).

## Syntax

```sql
ST_POLYGONFROMWKB(wkb [, srid] [, options])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_POLYFROMWKB`](st_polyfromwkb.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(ST_POLYGONFROMWKB(ST_ASBINARY(ST_GEOMFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))'))));
```

Result: `POLYGON((0 0,0 2,2 2,0 0))`.

## Related Functions

- [`ST_POLYFROMWKB`](st_polyfromwkb.md): the primary function with full documentation.

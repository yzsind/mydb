# `ST_MULTIPOLYGONFROMWKB`

## Description

Constructs a multipolygon from WKB; an alias of `ST_MPOLYFROMWKB`. This function shares the same implementation and semantics as [`ST_MPOLYFROMWKB`](st_mpolyfromwkb.md).

## Syntax

```sql
ST_MULTIPOLYGONFROMWKB(wkb [, srid] [, options])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_MPOLYFROMWKB`](st_mpolyfromwkb.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(ST_MULTIPOLYGONFROMWKB(ST_ASBINARY(ST_GEOMFROMTEXT('MULTIPOLYGON(((0 0, 0 2, 2 2, 0 0)))'))));
```

Result: `MULTIPOLYGON(((0 0,0 2,2 2,0 0)))`.

## Related Functions

- [`ST_MPOLYFROMWKB`](st_mpolyfromwkb.md): the primary function with full documentation.

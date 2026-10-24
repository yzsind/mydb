# `GEOMETRYCOLLECTION`

## Description

Constructs a geometry collection from geometry arguments; an alias of `GEOMCOLLECTION`. This function shares the same implementation and semantics as [`GEOMCOLLECTION`](geomcollection.md).

## Syntax

```sql
GEOMETRYCOLLECTION([geometry [, ...]])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`GEOMCOLLECTION`](geomcollection.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(GEOMETRYCOLLECTION(ST_GEOMFROMTEXT('POINT(1 2)'), ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)')));
```

Result: `GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,2 2))`.

## Related Functions

- [`GEOMCOLLECTION`](geomcollection.md): the primary function with full documentation.

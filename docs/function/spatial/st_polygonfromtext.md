# `ST_POLYGONFROMTEXT`

## Description

Constructs a polygon from WKT; an alias of `ST_POLYFROMTEXT`. This function shares the same implementation and semantics as [`ST_POLYFROMTEXT`](st_polyfromtext.md).

## Syntax

```sql
ST_POLYGONFROMTEXT(wkt [, srid] [, options])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_POLYFROMTEXT`](st_polyfromtext.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(ST_POLYGONFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))'));
```

Result: `POLYGON((0 0,0 2,2 2,0 0))`.

## Related Functions

- [`ST_POLYFROMTEXT`](st_polyfromtext.md): the primary function with full documentation.

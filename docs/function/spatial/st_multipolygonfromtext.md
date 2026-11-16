# `ST_MULTIPOLYGONFROMTEXT`

## Description

Constructs a multipolygon from WKT; an alias of `ST_MPOLYFROMTEXT`. This function shares the same implementation and semantics as [`ST_MPOLYFROMTEXT`](st_mpolyfromtext.md).

## Syntax

```sql
ST_MULTIPOLYGONFROMTEXT(wkt [, srid] [, options])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_MPOLYFROMTEXT`](st_mpolyfromtext.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(ST_MULTIPOLYGONFROMTEXT('MULTIPOLYGON(((0 0, 0 2, 2 2, 0 0)))'));
```

Result: `MULTIPOLYGON(((0 0,0 2,2 2,0 0)))`.

## Related Functions

- [`ST_MPOLYFROMTEXT`](st_mpolyfromtext.md): the primary function with full documentation.

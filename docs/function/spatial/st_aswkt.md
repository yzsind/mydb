# `ST_ASWKT`

## Description

Serializes a geometry to WKT; an alias of `ST_ASTEXT`. This function shares the same implementation and semantics as [`ST_ASTEXT`](st_astext.md).

## Syntax

```sql
ST_ASWKT(geometry)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_ASTEXT`](st_astext.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASWKT(ST_GEOMFROMTEXT('POINT(1 2)'));
```

Result: `POINT(1 2)`.

## Related Functions

- [`ST_ASTEXT`](st_astext.md): the primary function with full documentation.

# `ST_MULTILINESTRINGFROMWKB`

## Description

Constructs a multilinestring from WKB; an alias of `ST_MLINEFROMWKB`. This function shares the same implementation and semantics as [`ST_MLINEFROMWKB`](st_mlinefromwkb.md).

## Syntax

```sql
ST_MULTILINESTRINGFROMWKB(wkb [, srid] [, options])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_MLINEFROMWKB`](st_mlinefromwkb.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(ST_MULTILINESTRINGFROMWKB(ST_ASBINARY(ST_GEOMFROMTEXT('MULTILINESTRING((0 0, 1 1))'))));
```

Result: `MULTILINESTRING((0 0,1 1))`.

## Related Functions

- [`ST_MLINEFROMWKB`](st_mlinefromwkb.md): the primary function with full documentation.

# `ST_MULTILINESTRINGFROMTEXT`

## Description

Constructs a multilinestring from WKT; an alias of `ST_MLINEFROMTEXT`. This function shares the same implementation and semantics as [`ST_MLINEFROMTEXT`](st_mlinefromtext.md).

## Syntax

```sql
ST_MULTILINESTRINGFROMTEXT(wkt [, srid] [, options])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_MLINEFROMTEXT`](st_mlinefromtext.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(ST_MULTILINESTRINGFROMTEXT('MULTILINESTRING((0 0, 1 1))'));
```

Result: `MULTILINESTRING((0 0,1 1))`.

## Related Functions

- [`ST_MLINEFROMTEXT`](st_mlinefromtext.md): the primary function with full documentation.

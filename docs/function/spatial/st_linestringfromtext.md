# `ST_LINESTRINGFROMTEXT`

## Description

Constructs a linestring from WKT; an alias of `ST_LINEFROMTEXT`. This function shares the same implementation and semantics as [`ST_LINEFROMTEXT`](st_linefromtext.md).

## Syntax

```sql
ST_LINESTRINGFROMTEXT(wkt [, srid] [, options])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_LINEFROMTEXT`](st_linefromtext.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(ST_LINESTRINGFROMTEXT('LINESTRING(0 0, 2 2)'));
```

Result: `LINESTRING(0 0,2 2)`.

## Related Functions

- [`ST_LINEFROMTEXT`](st_linefromtext.md): the primary function with full documentation.

# `ST_LINESTRINGFROMWKB`

## Description

Constructs a linestring from WKB; an alias of `ST_LINEFROMWKB`. This function shares the same implementation and semantics as [`ST_LINEFROMWKB`](st_linefromwkb.md).

## Syntax

```sql
ST_LINESTRINGFROMWKB(wkb [, srid] [, options])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_LINEFROMWKB`](st_linefromwkb.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_ASTEXT(ST_LINESTRINGFROMWKB(ST_ASBINARY(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'))));
```

Result: `LINESTRING(0 0,2 2)`.

## Related Functions

- [`ST_LINEFROMWKB`](st_linefromwkb.md): the primary function with full documentation.

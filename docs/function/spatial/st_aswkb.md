# `ST_ASWKB`

## Description

Serializes a geometry to WKB; an alias of `ST_ASBINARY`. This function shares the same implementation and semantics as [`ST_ASBINARY`](st_asbinary.md).

## Syntax

```sql
ST_ASWKB(geometry)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_ASBINARY`](st_asbinary.md); refer to the primary function's documentation.

## Examples

```sql
SELECT HEX(ST_ASWKB(ST_GEOMFROMTEXT('POINT(1 2)')));
```

Result: Returns the hexadecimal text of the WKB for POINT(1 2).

## Related Functions

- [`ST_ASBINARY`](st_asbinary.md): the primary function with full documentation.

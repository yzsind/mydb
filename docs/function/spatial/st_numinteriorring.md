# `ST_NUMINTERIORRING`

## Description

Returns the number of interior rings of a polygon; an alias of `ST_NUMINTERIORRINGS`. This function shares the same implementation and semantics as [`ST_NUMINTERIORRINGS`](st_numinteriorrings.md).

## Syntax

```sql
ST_NUMINTERIORRING(geometry)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`ST_NUMINTERIORRINGS`](st_numinteriorrings.md); refer to the primary function's documentation.

## Examples

```sql
SELECT ST_NUMINTERIORRING(ST_GEOMFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))'));
```

Result: 0

## Related Functions

- [`ST_NUMINTERIORRINGS`](st_numinteriorrings.md): the primary function with full documentation.

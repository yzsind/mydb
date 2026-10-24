# `ST_POINTFROMTEXT`

## Description

Constructs a point from WKT.

## Syntax

```sql
ST_POINTFROMTEXT(wkt [, srid] [, options])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `wkt` | Yes | WKT string | The WKT matching the constructor's geometry type |
| `srid` | No | Non-negative integer | The SRID of the resulting geometry; defaults to 0 |
| `options` | No | Options string | Parsing options such as axis order |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_POINTFROMTEXT('POINT(1 2)'));
```

Result: `POINT(1 2)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

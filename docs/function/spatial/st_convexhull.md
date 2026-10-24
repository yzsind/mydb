# `ST_CONVEXHULL`

## Description

Returns the convex hull of a geometry.

## Syntax

```sql
ST_CONVEXHULL(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_CONVEXHULL(ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(POINT(1 2),POINT(2 3))')));
```

Result: `LINESTRING(1 2,2 3)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

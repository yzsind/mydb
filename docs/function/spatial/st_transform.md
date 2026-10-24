# `ST_TRANSFORM`

## Description

Transforms a geometry to the target spatial reference system.

## Syntax

```sql
ST_TRANSFORM(geometry, target_srid)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY with a valid SRID | The source geometry |
| `target_srid` | Yes | Non-negative integer | The target spatial reference system |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_TRANSFORM(ST_GEOMFROMTEXT('POINT(39.9 116.4)',4326),3857));
```

Result: `POINT(12957588.728337044 4851421.1751833595)` (floating-point formatting may vary in the last digits).

## Notes

- EPSG:4326 uses latitude/longitude axis order for WKT input and default WKT output here. GeoJSON instead supplies longitude/latitude coordinates. Use an explicit SRID-aware constructor to avoid confusing Cartesian POINT coordinates with geographic axes.

- Nonidentity transforms currently support only EPSG:4326 to EPSG:3857 and the reverse; equal-SRID input is returned unchanged. Other pairs return an explicit unsupported-transform error. A third options argument is not implemented and returns 1582/42000. The Web Mercator conversion clamps latitude to approximately ±85.05112878 degrees.
- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

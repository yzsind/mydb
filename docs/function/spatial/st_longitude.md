# `ST_LONGITUDE`

## Description

Returns the longitude of a geographic point.

## Syntax

```sql
ST_LONGITUDE(geometry [, new_value])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | POINT or GEOMETRY | The geometry to read or modify |
| `new_value` | No | Numeric value or SRID | When provided, returns the modified geometry |

## Return Value

Returns a numeric value; returns `NULL` when the spatial input is `NULL`. Setter overloads return the modified geometry.

## Examples

```sql
SELECT ST_LONGITUDE(ST_GEOMFROMTEXT('POINT(39.9 116.4)',4326));
```

Result: 116.4

## Notes

- EPSG:4326 uses latitude/longitude axis order for WKT input and default WKT output here. GeoJSON instead supplies longitude/latitude coordinates. Use an explicit SRID-aware constructor to avoid confusing Cartesian POINT coordinates with geographic axes.

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

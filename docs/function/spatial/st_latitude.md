# `ST_LATITUDE`

## Description

Returns the latitude of a geographic point.

## Syntax

```sql
ST_LATITUDE(geometry [, new_value])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | POINT | Point to inspect/modify; longitude/latitude accessors require a geographic SRS. |
| `new_value` | No | Finite numeric coordinate | New coordinate; setter returns a GEOMETRY rather than a number. |

## Return Value

With one argument, returns the latitude coordinate as DOUBLE; with two, returns a modified POINT GEOMETRY. SQL NULL input returns NULL. Geometry type, geographic-coordinate ranges and SRS requirements are validated.

## Examples

```sql
SELECT ST_LATITUDE(ST_GEOMFROMTEXT('POINT(39.9 116.4)',4326));
```

Result: 39.9

## Notes

- EPSG:4326 uses latitude/longitude axis order for WKT input and default WKT output here. GeoJSON instead supplies longitude/latitude coordinates. Use an explicit SRID-aware constructor to avoid confusing Cartesian POINT coordinates with geographic axes.

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_X`](st_x.md), [`ST_Y`](st_y.md), [`ST_LONGITUDE`](st_longitude.md), [`ST_SRID`](st_srid.md).

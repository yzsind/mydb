# `ST_GEOMFROMGEOJSON`

## Description

Constructs a geometry from GeoJSON.

## Syntax

```sql
ST_GEOMFROMGEOJSON(geojson [, options] [, srid])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geojson` | Yes | GeoJSON document | The geometry to parse |
| `options` | No | Integer | Options for handling invalid dimensions |
| `srid` | No | Non-negative integer | The SRID of the resulting geometry |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_GEOMFROMGEOJSON('{"type":"Point","coordinates":[1,2]}'));
```

Result: `POINT(2 1)`; the default SRID is 4326.

## Notes

- The default SRID is 4326. GeoJSON coordinates are longitude/latitude; the constructor converts them to the SRS axis order, which is latitude/longitude for EPSG:4326. WKT output therefore displays `(2 1)` for GeoJSON `[1,2]`.
- With an explicit Cartesian SRID, `ST_ASTEXT(ST_GEOMFROMGEOJSON('{"type":"Point","coordinates":[1,2]}',1,0))` returns `POINT(1 2)`.

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

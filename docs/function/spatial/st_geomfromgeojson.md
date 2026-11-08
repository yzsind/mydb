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

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- options defaults to 1 and must be 1..4. Option 1 rejects coordinates with extra dimensions; the current 2..4 forms discard extra dimensions and construct a 2D geometry.

## Related Functions

[`ST_GEOMFROMTEXT`](st_geomfromtext.md), [`ST_GEOMFROMWKB`](st_geomfromwkb.md), [`ST_ASTEXT`](st_astext.md), [`ST_ASBINARY`](st_asbinary.md).

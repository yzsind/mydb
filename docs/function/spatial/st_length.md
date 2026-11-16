# `ST_LENGTH`

## Description

Returns the length of a linear geometry.

## Syntax

```sql
ST_LENGTH(geometry [, unit])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |
| `unit` | No | Supported length-unit name | Converts the SRS length units; SRID 0 cannot establish a conversion |

## Return Value

Returns a DOUBLE length for LineString, MultiLineString, or GeometryCollection, and SQL NULL for other geometry kinds or SQL NULL input. Geographic SRS measurement is unsupported; an explicit unit must be valid for the SRS.

## Examples

```sql
SELECT ST_LENGTH(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'));
```

Result: approximately 2.8284271247461903

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_AREA`](st_area.md), [`ST_DISTANCE`](st_distance.md), [`ST_DISTANCE_SPHERE`](st_distance_sphere.md).

# `ST_AREA`

## Description

Returns the area of a polygon or multipolygon.

## Syntax

```sql
ST_AREA(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns a DOUBLE planar area in squared coordinate units, or SQL NULL for SQL NULL input. Geographic SRS area is unsupported and returns an explicit error.

## Examples

```sql
SELECT ST_AREA(ST_GEOMFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))'));
```

Result: 2

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_LENGTH`](st_length.md), [`ST_DISTANCE`](st_distance.md), [`ST_DISTANCE_SPHERE`](st_distance_sphere.md).

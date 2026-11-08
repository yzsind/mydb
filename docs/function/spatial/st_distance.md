# `ST_DISTANCE`

## Description

Returns the shortest distance between two geometries.

## Syntax

```sql
ST_DISTANCE(geometry1, geometry2 [, unit])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry1` | Yes | GEOMETRY | The first geometry |
| `geometry2` | Yes | GEOMETRY | The second geometry in the same SRS |
| `unit` | No | Distance unit name | The optional output unit |

## Return Value

Returns a DOUBLE planar distance, or SQL NULL for SQL NULL or empty inputs. Both geometries must have the same SRID. Geographic SRS distance is unsupported; ST_DISTANCE_SPHERE provides a separate spherical Point/MultiPoint calculation. An explicit unit converts supported SRS units; SRID 0 cannot establish that conversion.

## Examples

```sql
SELECT ST_DISTANCE(ST_GEOMFROMTEXT('POINT(1 2)'), ST_GEOMFROMTEXT('POINT(2 3)'));
```

Result: approximately 1.4142135623730951

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_AREA`](st_area.md), [`ST_LENGTH`](st_length.md), [`ST_DISTANCE_SPHERE`](st_distance_sphere.md).

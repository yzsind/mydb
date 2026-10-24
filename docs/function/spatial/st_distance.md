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

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

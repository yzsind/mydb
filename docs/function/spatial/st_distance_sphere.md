# `ST_DISTANCE_SPHERE`

## Description

Returns the distance between two geometries on a sphere model.

## Syntax

```sql
ST_DISTANCE_SPHERE(geometry1, geometry2 [, radius])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry1`, `geometry2` | Yes | POINT or MULTIPOINT with the same SRID | The shapes whose minimum spherical distance is measured |
| `radius` | No | Positive finite number | Sphere radius in meters; see the default below |

## Return Value

Returns a DOUBLE distance in meters, or SQL NULL when an argument is SQL NULL or no distance exists for an empty input.

## Examples

```sql
SELECT ST_DISTANCE_SPHERE(ST_SRID(POINT(0,0),4326), ST_SRID(POINT(1,0),4326));
```

Result: Returns the spherical distance, approximately 111 kilometers.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.
- Supports Cartesian SRID 0 interpreted as longitude/latitude degrees and geographic SRS axis order; projected SRSs are rejected. The default radius is 6370986 meters for both cases, not an ellipsoidal distance calculation. A nonpositive radius returns 3706.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

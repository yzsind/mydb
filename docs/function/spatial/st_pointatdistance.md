# `ST_POINTATDISTANCE`

## Description

Returns the point on a linestring at a given distance from the start point.

## Syntax

```sql
ST_POINTATDISTANCE(linestring, distance)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `linestring` | Yes | LINESTRING | The source linestring |
| `distance` | Yes | Non-negative numeric value | The distance from the start point |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_POINTATDISTANCE(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'), 1));
```

Result: `POINT(0.7071067811865475 0.7071067811865475)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- Coordinates use the input LineString and its length units. Single-point fractional interpolation accepts [0,1]; repeated-point interpolation requires (0,1]. Distance positions must lie within [0,line length]. Empty/zero-length/invalid inputs follow the geometry diagnostic rules. Interpolating many output points is bounded by governed memory.

## Related Functions

[`ST_LINEINTERPOLATEPOINT`](st_lineinterpolatepoint.md), [`ST_LINEINTERPOLATEPOINTS`](st_lineinterpolatepoints.md).

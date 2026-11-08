# `ST_LINEINTERPOLATEPOINTS`

## Description

Returns a multipoint of points interpolated along a linestring at a given interval.

## Syntax

```sql
ST_LINEINTERPOLATEPOINTS(linestring, fraction)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `linestring` | Yes | LINESTRING | The source linestring |
| `fraction` | Yes | Numeric value between 0 and 1 | The fraction along the linestring length |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_LINEINTERPOLATEPOINTS(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'), 0.5));
```

Result: `MULTIPOINT(1 1,2 2)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- Coordinates use the input LineString and its length units. Single-point fractional interpolation accepts [0,1]; repeated-point interpolation requires (0,1]. Distance positions must lie within [0,line length]. Empty/zero-length/invalid inputs follow the geometry diagnostic rules. Interpolating many output points is bounded by governed memory.

- A zero fraction (or zero-length line producing a zero step) is an out-of-range error. Fractions above 1 are errors. The first output point is one step from the start, and the endpoint is included only when reached by a whole step.

## Related Functions

[`ST_LINEINTERPOLATEPOINT`](st_lineinterpolatepoint.md), [`ST_POINTATDISTANCE`](st_pointatdistance.md).

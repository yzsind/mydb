# `ST_LINEINTERPOLATEPOINT`

## Description

Returns the point interpolated along a linestring at a given fraction.

## Syntax

```sql
ST_LINEINTERPOLATEPOINT(linestring, fraction)
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
SELECT ST_ASTEXT(ST_LINEINTERPOLATEPOINT(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'), 0.5));
```

Result: `POINT(1 1)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- Coordinates use the input LineString and its length units. Single-point fractional interpolation accepts [0,1]; repeated-point interpolation requires (0,1]. Distance positions must lie within [0,line length]. Empty/zero-length/invalid inputs follow the geometry diagnostic rules. Interpolating many output points is bounded by governed memory.

- Fraction 0 returns the start point and 1 returns the endpoint. A zero-length nonempty line returns its repeated coordinate for a valid fraction.

## Related Functions

[`ST_LINEINTERPOLATEPOINTS`](st_lineinterpolatepoints.md), [`ST_POINTATDISTANCE`](st_pointatdistance.md).

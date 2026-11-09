# `ST_CENTROID`

## Description

Returns the centroid of a geometry.

## Syntax

```sql
ST_CENTROID(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY | Point, linear, polygonal or collection input; the result uses the highest contributing dimension. |

## Return Value

Returns a POINT GEOMETRY with the input SRID, or SQL NULL for NULL/empty input with no centroid. The calculation prefers area-weighted polygon centroids, then length-weighted line midpoints, then the vertex average.

## Examples

```sql
SELECT ST_ASTEXT(ST_CENTROID(ST_GEOMFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))')));
```

Result: `POINT(0.6666666666666666 1.3333333333333333)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- In a mixed collection the highest available nondegenerate dimension supplies the centroid. The result is a POINT, not a bounding polygon or a validity repair.

## Related Functions

[`ST_CONVEXHULL`](st_convexhull.md), [`ST_ENVELOPE`](st_envelope.md), [`ST_MAKEENVELOPE`](st_makeenvelope.md), [`ST_SIMPLIFY`](st_simplify.md).

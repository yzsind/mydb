# `ST_ENVELOPE`

## Description

Returns the minimum bounding rectangle of a geometry.

## Syntax

```sql
ST_ENVELOPE(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY | Coordinates from which to form a bounding rectangle or convex hull. |

## Return Value

Returns the axis-aligned bounding GEOMETRY with the input SRID: POINT for coincident coordinates, LINESTRING for an axis-aligned degenerate rectangle, otherwise a rectangular POLYGON. NULL/empty input returns SQL NULL.

## Examples

```sql
SELECT ST_ASTEXT(ST_ENVELOPE(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)')));
```

Result: `POLYGON((0 0,2 0,2 2,0 2,0 0))`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- The envelope covers the coordinate bounds. It is not the convex hull of a diagonal or concave input, and can include areas outside the input geometry.

## Related Functions

[`ST_CENTROID`](st_centroid.md), [`ST_CONVEXHULL`](st_convexhull.md), [`ST_MAKEENVELOPE`](st_makeenvelope.md), [`ST_SIMPLIFY`](st_simplify.md).

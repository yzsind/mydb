# `ST_CONVEXHULL`

## Description

Returns the convex hull of a geometry.

## Syntax

```sql
ST_CONVEXHULL(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY | Coordinates from which to form a bounding rectangle or convex hull. |

## Return Value

Returns the vertex convex hull with the input SRID: POLYGON for a two-dimensional hull, LINESTRING for collinear coordinates, or POINT for coincident coordinates. SQL NULL or an empty input returns NULL.

## Examples

```sql
SELECT ST_ASTEXT(ST_CONVEXHULL(ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(POINT(1 2),POINT(2 3))')));
```

Result: `LINESTRING(1 2,2 3)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- Coincident vertices are deduplicated for the hull. A convex hull is distinct from the axis-aligned envelope and from topological validity repair.

## Related Functions

[`ST_CENTROID`](st_centroid.md), [`ST_ENVELOPE`](st_envelope.md), [`ST_MAKEENVELOPE`](st_makeenvelope.md), [`ST_SIMPLIFY`](st_simplify.md).

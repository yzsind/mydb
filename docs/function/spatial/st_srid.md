# `ST_SRID`

## Description

Returns the SRID of a geometry, or returns the geometry with the SRID set.

## Syntax

```sql
ST_SRID(geometry [, new_srid])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY | Geometry to inspect/retag. |
| `new_srid` | No | Nonnegative supported SRID | Target SRS identity; this is not a general projection transformation. |

## Return Value

Returns an integer; returns `NULL` when the spatial input is `NULL`. Setter overloads return the modified geometry.

## Examples

```sql
SELECT ST_SRID(ST_GEOMFROMTEXT('POINT(1 2)'));
```

Result: 0

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- Assigning/removing an SRS with a different axis order adjusts ordinate order in the current implementation. ST_SRID does not perform a projected-coordinate transformation; use ST_TRANSFORM for its supported projection pairs.

## Related Functions

[`ST_X`](st_x.md), [`ST_Y`](st_y.md), [`ST_LATITUDE`](st_latitude.md), [`ST_LONGITUDE`](st_longitude.md).

# `ST_EXTERIORRING`

## Description

Returns the exterior ring of a polygon.

## Syntax

```sql
ST_EXTERIORRING(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns the exterior LineString ring of a Polygon with its SRID, or SQL NULL for SQL NULL/empty/inapplicable input.

## Examples

```sql
SELECT ST_ASTEXT(ST_EXTERIORRING(ST_GEOMFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))')));
```

Result: `LINESTRING(0 0,0 2,2 2,0 0)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_INTERIORRINGN`](st_interiorringn.md), [`ST_NUMINTERIORRINGS`](st_numinteriorrings.md).

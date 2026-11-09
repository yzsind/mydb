# `ST_NUMPOINTS`

## Description

Returns the number of points in a linestring.

## Syntax

```sql
ST_NUMPOINTS(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns an integer point count for a LineString, or SQL NULL for SQL NULL or an inapplicable geometry type.

## Examples

```sql
SELECT ST_NUMPOINTS(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'));
```

Result: 2

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_STARTPOINT`](st_startpoint.md), [`ST_ENDPOINT`](st_endpoint.md), [`ST_POINTN`](st_pointn.md).

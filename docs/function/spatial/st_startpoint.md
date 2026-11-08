# `ST_STARTPOINT`

## Description

Returns the start point of a linestring.

## Syntax

```sql
ST_STARTPOINT(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns the first POINT of a LineString with the same SRID. SQL NULL, an empty input or an inapplicable geometry type returns NULL.

## Examples

```sql
SELECT ST_ASTEXT(ST_STARTPOINT(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)')));
```

Result: `POINT(0 0)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_ENDPOINT`](st_endpoint.md), [`ST_POINTN`](st_pointn.md), [`ST_NUMPOINTS`](st_numpoints.md).

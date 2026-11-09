# `ST_ENDPOINT`

## Description

Returns the endpoint of a linestring.

## Syntax

```sql
ST_ENDPOINT(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns the last POINT of a LineString with the same SRID. SQL NULL, an empty input or an inapplicable geometry type returns NULL.

## Examples

```sql
SELECT ST_ASTEXT(ST_ENDPOINT(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)')));
```

Result: `POINT(2 2)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_STARTPOINT`](st_startpoint.md), [`ST_POINTN`](st_pointn.md), [`ST_NUMPOINTS`](st_numpoints.md).

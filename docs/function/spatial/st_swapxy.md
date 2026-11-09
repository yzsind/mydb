# `ST_SWAPXY`

## Description

Swaps the X and Y coordinates of every point in a geometry.

## Syntax

```sql
ST_SWAPXY(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_SWAPXY(ST_GEOMFROMTEXT('POINT(1 2)')));
```

Result: `POINT(2 1)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_X`](st_x.md), [`ST_Y`](st_y.md), [`ST_LATITUDE`](st_latitude.md), [`ST_LONGITUDE`](st_longitude.md).

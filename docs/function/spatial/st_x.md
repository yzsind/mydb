# `ST_X`

## Description

Returns the X coordinate of a point, or returns the point with the X coordinate set.

## Syntax

```sql
ST_X(geometry [, new_value])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | POINT | Point to inspect/modify; longitude/latitude accessors require a geographic SRS. |
| `new_value` | No | Finite numeric coordinate | New coordinate; setter returns a GEOMETRY rather than a number. |

## Return Value

With one argument, returns the X coordinate as DOUBLE; with two, returns a modified POINT GEOMETRY. SQL NULL input returns NULL. Geometry type, geographic-coordinate ranges and SRS requirements are validated.

## Examples

```sql
SELECT ST_X(ST_GEOMFROMTEXT('POINT(1 2)'));
```

Result: 1

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_Y`](st_y.md), [`ST_LATITUDE`](st_latitude.md), [`ST_LONGITUDE`](st_longitude.md), [`ST_SRID`](st_srid.md).

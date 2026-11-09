# `ST_GEOHASH`

## Description

Generates a geohash from a point or latitude/longitude values.

## Syntax

```sql
ST_GEOHASH(longitude_or_point, latitude_or_max_length [, max_length])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `longitude_or_point` | Yes | Longitude or POINT | The longitude or the geographic point |
| `latitude_or_max_length` | Yes | Latitude or integer | The latitude or the maximum geohash length |
| `max_length` | No | Integer | The maximum length for the latitude/longitude form |

## Return Value

Returns a string; returns `NULL` when the spatial input is `NULL`.

## Examples

```sql
SELECT ST_GEOHASH(116.4, 39.9, 8);
```

Result: wx4fbxxf

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- max_length must be 1..100, longitude within [-180,180] and latitude within [-90,90]. The point overload accepts SRID 0 or 4326 only; the text encodes a cell rather than preserving an exact coordinate.

## Related Functions

[`ST_LATFROMGEOHASH`](st_latfromgeohash.md), [`ST_LONGFROMGEOHASH`](st_longfromgeohash.md), [`ST_POINTFROMGEOHASH`](st_pointfromgeohash.md).

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

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

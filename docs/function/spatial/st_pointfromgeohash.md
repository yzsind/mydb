# `ST_POINTFROMGEOHASH`

## Description

Constructs a point from a geohash.

## Syntax

```sql
ST_POINTFROMGEOHASH(geohash, srid)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geohash` | Yes | String | The geohash to decode |
| `srid` | Yes | Non-negative integer | The SRID of the resulting point |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_POINTFROMGEOHASH('wx4fbxxf',0));
```

Result: `POINT(116.39997482299805 39.90002632141113)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

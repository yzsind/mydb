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

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_GEOHASH`](st_geohash.md), [`ST_LATFROMGEOHASH`](st_latfromgeohash.md), [`ST_LONGFROMGEOHASH`](st_longfromgeohash.md).

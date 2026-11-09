# `ST_LONGFROMGEOHASH`

## Description

Decodes the longitude from a geohash.

## Syntax

```sql
ST_LONGFROMGEOHASH(geohash)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geohash` | Yes | String | The geohash to decode |

## Return Value

Returns the DOUBLE longitude of the geohash cell center, or SQL NULL for SQL NULL input. Invalid geohash text returns an error.

## Examples

```sql
SELECT ST_LONGFROMGEOHASH('wx4fbxxf');
```

Result:

```text
116.39997482299805
```

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_GEOHASH`](st_geohash.md), [`ST_LATFROMGEOHASH`](st_latfromgeohash.md), [`ST_POINTFROMGEOHASH`](st_pointfromgeohash.md).

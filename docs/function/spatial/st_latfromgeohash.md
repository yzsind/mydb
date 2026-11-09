# `ST_LATFROMGEOHASH`

## Description

Decodes the latitude from a geohash.

## Syntax

```sql
ST_LATFROMGEOHASH(geohash)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geohash` | Yes | String | The geohash to decode |

## Return Value

Returns the DOUBLE latitude of the geohash cell center, or SQL NULL for SQL NULL input. Invalid geohash text returns an error.

## Examples

```sql
SELECT ST_LATFROMGEOHASH('wx4fbxxf');
```

Result:

```text
39.90002632141113
```

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_GEOHASH`](st_geohash.md), [`ST_LONGFROMGEOHASH`](st_longfromgeohash.md), [`ST_POINTFROMGEOHASH`](st_pointfromgeohash.md).

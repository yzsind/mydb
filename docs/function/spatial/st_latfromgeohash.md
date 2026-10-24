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

Result: Returns the latitude of the geohash center.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

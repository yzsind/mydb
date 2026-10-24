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

Result: Returns the longitude of the geohash center.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

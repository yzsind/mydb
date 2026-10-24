# `ST_EXTERIORRING`

## Description

Returns the exterior ring of a polygon.

## Syntax

```sql
ST_EXTERIORRING(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_EXTERIORRING(ST_GEOMFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))')));
```

Result: `LINESTRING(0 0,0 2,2 2,0 0)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

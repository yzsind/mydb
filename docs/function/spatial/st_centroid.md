# `ST_CENTROID`

## Description

Returns the centroid of a geometry.

## Syntax

```sql
ST_CENTROID(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_CENTROID(ST_GEOMFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))')));
```

Result: `POINT(0.6666666666666666 1.3333333333333333)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

# `ST_NUMPOINTS`

## Description

Returns the number of points in a linestring.

## Syntax

```sql
ST_NUMPOINTS(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns an integer point count for a LineString, or SQL NULL for SQL NULL or an inapplicable geometry type.

## Examples

```sql
SELECT ST_NUMPOINTS(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'));
```

Result: 2

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

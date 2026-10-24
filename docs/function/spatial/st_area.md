# `ST_AREA`

## Description

Returns the area of a polygon or multipolygon.

## Syntax

```sql
ST_AREA(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns a DOUBLE planar area in squared coordinate units, or SQL NULL for SQL NULL input. Geographic SRS area is unsupported and returns an explicit error.

## Examples

```sql
SELECT ST_AREA(ST_GEOMFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))'));
```

Result: 2

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

# `ST_DIMENSION`

## Description

Returns the topological dimension of a geometry.

## Syntax

```sql
ST_DIMENSION(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns an integer geometry dimension, or SQL NULL for SQL NULL input.

## Examples

```sql
SELECT ST_DIMENSION(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'));
```

Result: 1

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

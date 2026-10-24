# `ST_GEOMETRYTYPE`

## Description

Returns the type name of a geometry.

## Syntax

```sql
ST_GEOMETRYTYPE(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns a string; returns `NULL` when the spatial input is `NULL`.

## Examples

```sql
SELECT ST_GEOMETRYTYPE(ST_GEOMFROMTEXT('POINT(1 2)'));
```

Result: POINT

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

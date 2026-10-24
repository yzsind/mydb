# `ST_NUMGEOMETRIES`

## Description

Returns the number of geometries in a geometry collection.

## Syntax

```sql
ST_NUMGEOMETRIES(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns an integer geometry count, or SQL NULL for SQL NULL input.

## Examples

```sql
SELECT ST_NUMGEOMETRIES(ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(POINT(1 2),POINT(2 3))'));
```

Result: 2

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

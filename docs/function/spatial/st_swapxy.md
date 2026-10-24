# `ST_SWAPXY`

## Description

Swaps the X and Y coordinates of every point in a geometry.

## Syntax

```sql
ST_SWAPXY(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_SWAPXY(ST_GEOMFROMTEXT('POINT(1 2)')));
```

Result: `POINT(2 1)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

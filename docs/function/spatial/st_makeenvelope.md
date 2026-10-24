# `ST_MAKEENVELOPE`

## Description

Constructs a rectangular envelope from two corner points.

## Syntax

```sql
ST_MAKEENVELOPE(point1, point2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `point1` | Yes | POINT | One corner of the rectangle |
| `point2` | Yes | POINT | The diagonally opposite corner of the rectangle |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_MAKEENVELOPE(ST_GEOMFROMTEXT('POINT(1 2)'), ST_GEOMFROMTEXT('POINT(2 3)')));
```

Result: `POLYGON((1 2,2 2,2 3,1 3,1 2))`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

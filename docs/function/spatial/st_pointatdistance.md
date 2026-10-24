# `ST_POINTATDISTANCE`

## Description

Returns the point on a linestring at a given distance from the start point.

## Syntax

```sql
ST_POINTATDISTANCE(linestring, distance)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `linestring` | Yes | LINESTRING | The source linestring |
| `distance` | Yes | Non-negative numeric value | The distance from the start point |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_POINTATDISTANCE(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'), 1));
```

Result: `POINT(0.7071067811865475 0.7071067811865475)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

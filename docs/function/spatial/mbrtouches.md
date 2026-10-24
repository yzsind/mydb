# `MBRTOUCHES`

## Description

Determines whether the minimum bounding rectangles of two geometries touch.

## Syntax

```sql
MBRTOUCHES(geometry1, geometry2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry1` | Yes | GEOMETRY | The first geometry |
| `geometry2` | Yes | GEOMETRY | The second compatible geometry |

## Return Value

Returns `1` when the condition holds and `0` otherwise; `NULL` input usually returns `NULL`.

## Examples

```sql
SELECT MBRTOUCHES(ST_GEOMFROMTEXT('POINT(1 2)'), ST_GEOMFROMTEXT('POINT(2 3)'));
```

Result: returns 0 or 1, indicating whether the spatial relationship holds.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

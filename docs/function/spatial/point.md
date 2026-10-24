# `POINT`

## Description

Constructs a point from X and Y coordinates.

## Syntax

```sql
POINT(x, y)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `x` | Yes | Numeric | The X coordinate |
| `y` | Yes | Numeric | The Y coordinate |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(POINT(1, 2));
```

Result: `POINT(1 2)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

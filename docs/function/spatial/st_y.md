# `ST_Y`

## Description

Returns the Y coordinate of a point, or returns the point with the Y coordinate set.

## Syntax

```sql
ST_Y(geometry [, new_value])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | POINT or GEOMETRY | The geometry to read or modify |
| `new_value` | No | Numeric value or SRID | When provided, returns the modified geometry |

## Return Value

Returns a numeric value; returns `NULL` when the spatial input is `NULL`. Setter overloads return the modified geometry.

## Examples

```sql
SELECT ST_Y(ST_GEOMFROMTEXT('POINT(1 2)'));
```

Result: 2

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

# `ST_EQUALS`

## Description

Determines whether two geometries are topologically equal.

## Syntax

```sql
ST_EQUALS(geometry1, geometry2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry1` | Yes | GEOMETRY | The first geometry |
| `geometry2` | Yes | GEOMETRY | The second compatible geometry |

## Return Value

Returns `1` when the condition holds and `0` otherwise; returns `NULL` when the spatial input is `NULL`.

## Examples

```sql
SELECT ST_EQUALS(ST_GEOMFROMTEXT('POINT(1 2)'), ST_GEOMFROMTEXT('POINT(2 3)'));
```

Result: returns 0 or 1, indicating whether the spatial relationship holds.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

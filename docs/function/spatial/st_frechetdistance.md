# `ST_FRECHETDISTANCE`

## Description

Returns the discrete Fréchet distance between two geometries.

## Syntax

```sql
ST_FRECHETDISTANCE(geometry1, geometry2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry1` | Yes | GEOMETRY | The first geometry |
| `geometry2` | Yes | GEOMETRY | The second compatible geometry |

## Return Value

Returns a numeric value; returns `NULL` when the spatial input is `NULL`. Setter overloads return the modified geometry.

## Examples

```sql
SELECT ST_FRECHETDISTANCE(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'), ST_GEOMFROMTEXT('LINESTRING(0 1, 2 3)'));
```

Result: Returns the discrete Fréchet distance between the two linestrings.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

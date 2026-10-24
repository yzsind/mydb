# `ST_HAUSDORFFDISTANCE`

## Description

Returns the discrete Hausdorff distance between two geometries.

## Syntax

```sql
ST_HAUSDORFFDISTANCE(geometry1, geometry2)
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
SELECT ST_HAUSDORFFDISTANCE(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'), ST_GEOMFROMTEXT('LINESTRING(0 1, 2 3)'));
```

Result: Returns the discrete Hausdorff distance between the two linestrings.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

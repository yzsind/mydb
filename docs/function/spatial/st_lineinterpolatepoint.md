# `ST_LINEINTERPOLATEPOINT`

## Description

Returns the point interpolated along a linestring at a given fraction.

## Syntax

```sql
ST_LINEINTERPOLATEPOINT(linestring, fraction)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `linestring` | Yes | LINESTRING | The source linestring |
| `fraction` | Yes | Numeric value between 0 and 1 | The fraction along the linestring length |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_LINEINTERPOLATEPOINT(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'), 0.5));
```

Result: `POINT(1 1)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

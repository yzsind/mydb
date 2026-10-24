# `ST_SIMPLIFY`

## Description

Simplifies a geometry using a tolerance.

## Syntax

```sql
ST_SIMPLIFY(geometry, tolerance)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY | The geometry to simplify |
| `tolerance` | Yes | Non-negative numeric value | The simplification tolerance |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_SIMPLIFY(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'), 0.1));
```

Result: `LINESTRING(0 0,2 2)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

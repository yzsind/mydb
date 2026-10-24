# `MULTIPOLYGON`

## Description

Constructs a multipolygon from a sequence of polygons.

## Syntax

```sql
MULTIPOLYGON(polygon [, ...])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `polygon` | Yes, repeatable | POLYGON | The polygons that make up the result |

## Return Value

Returns a GEOMETRY value built from Polygon members sharing one SRID. SQL NULL and other non-GEOMETRY arguments return an error rather than SQL NULL.

## Examples

```sql
SELECT ST_ASTEXT(MULTIPOLYGON(ST_GEOMFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))')));
```

Result: `MULTIPOLYGON(((0 0,0 2,2 2,0 0)))`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

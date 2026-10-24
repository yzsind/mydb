# `LINESTRING`

## Description

Constructs a linestring from a sequence of points.

## Syntax

```sql
LINESTRING(point [, ...])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `point` | Yes, repeatable | POINT | The points that make up the result; their count and validity must satisfy the geometry type requirements |

## Return Value

Returns a GEOMETRY value built from the POINT members, which must share an SRID. SQL NULL and other non-GEOMETRY arguments return an error rather than SQL NULL.

## Examples

```sql
SELECT ST_ASTEXT(LINESTRING(ST_GEOMFROMTEXT('POINT(1 2)'), ST_GEOMFROMTEXT('POINT(2 3)')));
```

Result: `LINESTRING(1 2,2 3)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

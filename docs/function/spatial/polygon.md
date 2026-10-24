# `POLYGON`

## Description

Constructs a polygon from closed linestrings.

## Syntax

```sql
POLYGON(linestring [, ...])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `linestring` | Yes, repeatable | LINESTRING | The linestrings or rings that make up the result |

## Return Value

Returns a GEOMETRY value built from the LineString rings, which must share an SRID. SQL NULL and other non-GEOMETRY arguments return an error rather than SQL NULL.

## Examples

```sql
SELECT ST_ASTEXT(POLYGON(ST_GEOMFROMTEXT('LINESTRING(0 0,0 2,2 2,0 0)')));
```

Result: `POLYGON((0 0,0 2,2 2,0 0))`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

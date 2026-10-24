# `MULTILINESTRING`

## Description

Constructs a multilinestring from a sequence of linestrings.

## Syntax

```sql
MULTILINESTRING(linestring [, ...])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `linestring` | Yes, repeatable | LINESTRING | The linestrings or rings that make up the result |

## Return Value

Returns a GEOMETRY value built from LineString members sharing one SRID. SQL NULL and other non-GEOMETRY arguments return an error rather than SQL NULL.

## Examples

```sql
SELECT ST_ASTEXT(MULTILINESTRING(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'), ST_GEOMFROMTEXT('LINESTRING(0 1, 2 3)')));
```

Result: `MULTILINESTRING((0 0,2 2),(0 1,2 3))`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

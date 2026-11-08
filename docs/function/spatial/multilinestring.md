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

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- Members must have the requested geometry type and share an SRID. A nonempty LineString needs at least two points; Polygon rings must be closed and have at least four coordinates including the closure. Constructor arguments are not silently skipped when SQL NULL.

## Related Functions

[`POINT`](point.md), [`LINESTRING`](linestring.md), [`POLYGON`](polygon.md), [`MULTIPOINT`](multipoint.md).

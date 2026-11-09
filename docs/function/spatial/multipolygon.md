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

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- Members must have the requested geometry type and share an SRID. A nonempty LineString needs at least two points; Polygon rings must be closed and have at least four coordinates including the closure. Constructor arguments are not silently skipped when SQL NULL.

## Related Functions

[`POINT`](point.md), [`LINESTRING`](linestring.md), [`POLYGON`](polygon.md), [`MULTIPOINT`](multipoint.md).

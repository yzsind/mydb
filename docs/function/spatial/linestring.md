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

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- Members must have the requested geometry type and share an SRID. A nonempty LineString needs at least two points; Polygon rings must be closed and have at least four coordinates including the closure. Constructor arguments are not silently skipped when SQL NULL.

## Related Functions

[`POINT`](point.md), [`POLYGON`](polygon.md), [`MULTIPOINT`](multipoint.md), [`MULTILINESTRING`](multilinestring.md).

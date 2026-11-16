# `GEOMCOLLECTION`

## Description

Constructs a geometry collection from geometry arguments.

## Syntax

```sql
GEOMCOLLECTION([geometry [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | No, repeatable | GEOMETRY | Collection members, all with the same SRID |

## Return Value

Returns a GEOMETRY value. With no arguments, returns an empty GeometryCollection with SRID 0. SQL NULL and other non-GEOMETRY arguments are errors; they are not silently omitted.

## Examples

```sql
SELECT ST_ASTEXT(GEOMCOLLECTION(ST_GEOMFROMTEXT('POINT(1 2)'), ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)')));
```

Result: `GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,2 2))`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

See [`GEOMETRYCOLLECTION`](geometrycollection.md).

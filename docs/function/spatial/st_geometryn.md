# `ST_GEOMETRYN`

## Description

Returns the Nth geometry in a geometry collection.

## Syntax

```sql
ST_GEOMETRYN(geometry, n)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | Matching composite geometry | The source geometry |
| `n` | Yes | Positive integer | The 1-based member position |

## Return Value

Returns the one-based member GEOMETRY for a Multi geometry/GeometryCollection. A single geometry at n=1 returns itself. SQL NULL or a nonexistent/nonpositive member position returns NULL. The member keeps the source SRID.

## Examples

```sql
SELECT ST_ASTEXT(ST_GEOMETRYN(ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(POINT(1 2),POINT(2 3))'), 2));
```

Result: `POINT(2 3)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_NUMGEOMETRIES`](st_numgeometries.md), [`ST_GEOMETRYTYPE`](st_geometrytype.md), [`ST_DIMENSION`](st_dimension.md).

# `ST_DIMENSION`

## Description

Returns the topological dimension of a geometry.

## Syntax

```sql
ST_DIMENSION(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns integer topological dimension 0 for points, 1 for linear geometries or 2 for surfaces, taking the greatest dimension in a collection. SQL NULL or an empty geometry has no dimension and returns NULL.

## Examples

```sql
SELECT ST_DIMENSION(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'));
```

Result: 1

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_GEOMETRYN`](st_geometryn.md), [`ST_NUMGEOMETRIES`](st_numgeometries.md), [`ST_GEOMETRYTYPE`](st_geometrytype.md).

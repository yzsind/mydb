# `ST_NUMGEOMETRIES`

## Description

Returns the number of geometries in a geometry collection.

## Syntax

```sql
ST_NUMGEOMETRIES(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns an integer member count: a nonempty single geometry counts as 1; Multi geometries/collections use their member count. SQL NULL returns NULL.

## Examples

```sql
SELECT ST_NUMGEOMETRIES(ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(POINT(1 2),POINT(2 3))'));
```

Result: 2

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_GEOMETRYN`](st_geometryn.md), [`ST_GEOMETRYTYPE`](st_geometrytype.md), [`ST_DIMENSION`](st_dimension.md).

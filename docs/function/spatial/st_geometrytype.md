# `ST_GEOMETRYTYPE`

## Description

Returns the type name of a geometry.

## Syntax

```sql
ST_GEOMETRYTYPE(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns a string; returns `NULL` when the spatial input is `NULL`.

## Examples

```sql
SELECT ST_GEOMETRYTYPE(ST_GEOMFROMTEXT('POINT(1 2)'));
```

Result: POINT

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_GEOMETRYN`](st_geometryn.md), [`ST_NUMGEOMETRIES`](st_numgeometries.md), [`ST_DIMENSION`](st_dimension.md).

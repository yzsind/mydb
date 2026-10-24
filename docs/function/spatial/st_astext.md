# `ST_ASTEXT`

## Description

Serializes a geometry to WKT.

## Syntax

```sql
ST_ASTEXT(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns a string; returns `NULL` when the spatial input is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_GEOMFROMTEXT('POINT(1 2)'));
```

Result: `POINT(1 2)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

See [`ST_ASWKT`](st_aswkt.md).

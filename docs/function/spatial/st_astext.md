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

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- Output follows the [serializer boundary](README.md#construction-and-coordinate-order): single-geometry output form only, and the optional MySQL axis-order output argument is rejected.

## Related Functions

See [`ST_ASWKT`](st_aswkt.md).

# `ST_GEOMFROMTEXT`

## Description

Constructs a geometry from WKT.

## Syntax

```sql
ST_GEOMFROMTEXT(wkt [, srid] [, options])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `wkt` | Yes | WKT string | The WKT matching the constructor's geometry type |
| `srid` | No | Non-negative integer | The SRID of the resulting geometry; defaults to 0 |
| `options` | No | Options string | Parsing options such as axis order |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_GEOMFROMTEXT('POINT(1 2)'));
```

Result: `POINT(1 2)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- srid defaults to 0. options accepts axis-order=srid-defined (default), axis-order=lat-long or axis-order=long-lat. See [construction and coordinate order](README.md#construction-and-coordinate-order) for SRS requirements and WKB versus internal geometry bytes.

## Related Functions

See [`ST_GEOMETRYFROMTEXT`](st_geometryfromtext.md).

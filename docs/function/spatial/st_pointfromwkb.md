# `ST_POINTFROMWKB`

## Description

Constructs a point from WKB.

## Syntax

```sql
ST_POINTFROMWKB(wkb [, srid] [, options])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `wkb` | Yes | WKB binary string | The WKB matching the constructor's geometry type |
| `srid` | No | Non-negative integer | The SRID of the resulting geometry; defaults to 0 |
| `options` | No | Options string | Parsing options such as axis order |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_POINTFROMWKB(ST_ASBINARY(ST_GEOMFROMTEXT('POINT(1 2)'))));
```

Result: `POINT(1 2)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- srid defaults to 0. options accepts axis-order=srid-defined (default), axis-order=lat-long or axis-order=long-lat. See [construction and coordinate order](README.md#construction-and-coordinate-order) for SRS requirements and WKB versus internal geometry bytes.

## Related Functions

[`ST_GEOMFROMTEXT`](st_geomfromtext.md), [`ST_GEOMFROMWKB`](st_geomfromwkb.md), [`ST_ASTEXT`](st_astext.md).

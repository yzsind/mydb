# `ST_ASGEOJSON`

## Description

Serializes a geometry to GeoJSON.

## Syntax

```sql
ST_ASGEOJSON(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns a JSON document containing the GeoJSON representation; returns SQL NULL when the spatial input is SQL NULL. The protocol renders the document as JSON text.

## Examples

```sql
SELECT ST_ASGEOJSON(ST_GEOMFROMTEXT('POINT(1 2)'));
```

Result: {"type": "Point", "coordinates": [1, 2]}

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.
- Optional precision/options arguments are recognized but unsupported (1235); only the one-argument form is implemented.

## Related Functions

[`ST_GEOMFROMTEXT`](st_geomfromtext.md), [`ST_GEOMFROMWKB`](st_geomfromwkb.md), [`ST_ASTEXT`](st_astext.md), [`ST_ASBINARY`](st_asbinary.md).

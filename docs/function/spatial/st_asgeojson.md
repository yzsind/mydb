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

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.
- Optional precision/options arguments are recognized but unsupported (1235); only the one-argument form is implemented.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

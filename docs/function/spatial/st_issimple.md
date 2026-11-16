# `ST_ISSIMPLE`

## Description

Determines whether a geometry is simple.

## Syntax

```sql
ST_ISSIMPLE(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns integer 1 when the geometry satisfies the implemented simplicity rules, 0 otherwise, or SQL NULL for SQL NULL input. Polygon simplicity is separate from polygon validity; use ST_ISVALID to check the latter.

## Examples

```sql
SELECT ST_ISSIMPLE(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'));
```

Result: 1

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_ISVALID`](st_isvalid.md), [`ST_VALIDATE`](st_validate.md), [`ST_ISCLOSED`](st_isclosed.md), [`ST_ISEMPTY`](st_isempty.md).

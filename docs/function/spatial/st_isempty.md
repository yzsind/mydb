# `ST_ISEMPTY`

## Description

Determines whether a geometry is empty.

## Syntax

```sql
ST_ISEMPTY(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns integer 1 for an empty geometry, 0 for a nonempty geometry, or SQL NULL for SQL NULL input. Empty geometry is a spatial value, not SQL NULL.

## Examples

```sql
SELECT ST_ISEMPTY(ST_GEOMFROMTEXT('POINT(1 2)'));
```

Result: 0

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_ISVALID`](st_isvalid.md), [`ST_VALIDATE`](st_validate.md), [`ST_ISSIMPLE`](st_issimple.md), [`ST_ISCLOSED`](st_isclosed.md).

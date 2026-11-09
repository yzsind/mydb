# `ST_ISVALID`

## Description

Determines whether a geometry is valid.

## Syntax

```sql
ST_ISVALID(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns integer 1 for valid topology, 0 for invalid topology, or SQL NULL for SQL NULL input. Invalid binary encoding and SRS validation can raise errors before the topological check.

## Examples

```sql
SELECT ST_ISVALID(ST_GEOMFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))'));
```

Result: 1

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_VALIDATE`](st_validate.md), [`ST_ISSIMPLE`](st_issimple.md), [`ST_ISCLOSED`](st_isclosed.md), [`ST_ISEMPTY`](st_isempty.md).

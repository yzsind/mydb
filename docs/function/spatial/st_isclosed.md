# `ST_ISCLOSED`

## Description

Determines whether a linestring or multilinestring is closed.

## Syntax

```sql
ST_ISCLOSED(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns integer 1 for a closed LineString/MultiLineString, 0 when a linear input is not closed, or SQL NULL for SQL NULL/inapplicable geometry types such as POINT.

## Examples

```sql
SELECT ST_ISCLOSED(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'));
```

Result: 0

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_ISVALID`](st_isvalid.md), [`ST_VALIDATE`](st_validate.md), [`ST_ISSIMPLE`](st_issimple.md), [`ST_ISEMPTY`](st_isempty.md).

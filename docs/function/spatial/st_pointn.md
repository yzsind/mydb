# `ST_POINTN`

## Description

Returns the Nth point in a linestring.

## Syntax

```sql
ST_POINTN(geometry, n)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | Matching composite geometry | The source geometry |
| `n` | Yes | Positive integer | The 1-based member position |

## Return Value

Returns the one-based point of a LineString, preserving SRID. SQL NULL, an inapplicable type, or an out-of-range/nonpositive position returns NULL.

## Examples

```sql
SELECT ST_ASTEXT(ST_POINTN(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'), 2));
```

Result: `POINT(2 2)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_STARTPOINT`](st_startpoint.md), [`ST_ENDPOINT`](st_endpoint.md), [`ST_NUMPOINTS`](st_numpoints.md).

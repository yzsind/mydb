# `ST_INTERIORRINGN`

## Description

Returns the Nth interior ring of a polygon.

## Syntax

```sql
ST_INTERIORRINGN(geometry, n)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | Matching composite geometry | The source geometry |
| `n` | Yes | Positive integer | The 1-based member position |

## Return Value

Returns the one-based interior LineString ring of a Polygon, preserving SRID. The exterior ring is not counted. SQL NULL, an inapplicable type, or an absent/nonpositive ring position returns NULL.

## Examples

```sql
SELECT ST_ASTEXT(ST_INTERIORRINGN(ST_GEOMFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))'), 1));
```

Result:

```text
NULL
```

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_EXTERIORRING`](st_exteriorring.md), [`ST_NUMINTERIORRINGS`](st_numinteriorrings.md).

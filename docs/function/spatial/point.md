# `POINT`

## Description

Constructs a point from X and Y coordinates.

## Syntax

```sql
POINT(x, y)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `x` | Yes | Numeric | The X coordinate |
| `y` | Yes | Numeric | The Y coordinate |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(POINT(1, 2));
```

Result: `POINT(1 2)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`LINESTRING`](linestring.md), [`POLYGON`](polygon.md), [`MULTIPOINT`](multipoint.md), [`MULTILINESTRING`](multilinestring.md).

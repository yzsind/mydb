# `MBRTOUCHES`

## Description

Determines whether the minimum bounding rectangles of two geometries touch.

## Syntax

```sql
MBRTOUCHES(geometry1, geometry2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry1` | Yes | GEOMETRY | The first geometry |
| `geometry2` | Yes | GEOMETRY | The second compatible geometry |

## Return Value

Returns integer 1/0 when the minimum-bounding-rectangle relationship holds/does not hold. SQL NULL or an empty geometry returns SQL NULL. Both geometries must have the same SRID; this tests rectangles, not the exact topology of the shapes.

## Examples

```sql
SELECT MBRTOUCHES(ST_GEOMFROMTEXT('POINT(1 2)'),ST_GEOMFROMTEXT('POINT(2 3)'));
```

Result:

```text
0
```

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`MBRCONTAINS`](mbrcontains.md), [`MBRWITHIN`](mbrwithin.md), [`MBRCOVERS`](mbrcovers.md), [`MBRCOVEREDBY`](mbrcoveredby.md).

# `ST_WITHIN`

## Description

Determines whether the first geometry is within the second geometry.

## Syntax

```sql
ST_WITHIN(geometry1, geometry2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry1` | Yes | GEOMETRY | The first geometry |
| `geometry2` | Yes | GEOMETRY | The second compatible geometry |

## Return Value

Returns `1` when the condition holds and `0` otherwise; returns `NULL` when the spatial input is `NULL`.

## Examples

```sql
SELECT ST_WITHIN(ST_GEOMFROMTEXT('POINT(1 2)'),ST_GEOMFROMTEXT('POINT(2 3)'));
```

Result:

```text
0
```

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_CONTAINS`](st_contains.md), [`ST_TOUCHES`](st_touches.md), [`ST_CROSSES`](st_crosses.md), [`ST_INTERSECTS`](st_intersects.md).

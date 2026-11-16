# `ST_TOUCHES`

## Description

Determines whether two geometries topologically touch.

## Syntax

```sql
ST_TOUCHES(geometry1, geometry2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry1` | Yes | GEOMETRY | The first geometry |
| `geometry2` | Yes | GEOMETRY | The second compatible geometry |

## Return Value

Returns integer 1/0 when the touching relation holds/does not hold for an applicable geometry combination. SQL NULL, empty/inapplicable combinations such as Point/Point can return SQL NULL. Both geometries must have the same SRID.

## Examples

```sql
SELECT ST_TOUCHES(ST_GEOMFROMTEXT('LINESTRING(0 0,1 1)'),ST_GEOMFROMTEXT('LINESTRING(1 1,2 0)')),
       ST_TOUCHES(POINT(1,2),POINT(2,3));
```

Result:

```text
1	NULL
```

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_CONTAINS`](st_contains.md), [`ST_WITHIN`](st_within.md), [`ST_CROSSES`](st_crosses.md), [`ST_INTERSECTS`](st_intersects.md).

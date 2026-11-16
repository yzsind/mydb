# `ST_SIMPLIFY`

## Description

Simplifies a geometry using a tolerance.

## Syntax

```sql
ST_SIMPLIFY(geometry, tolerance)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY | The geometry to simplify |
| `tolerance` | Yes | Non-negative numeric value | The simplification tolerance |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_SIMPLIFY(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'), 0.1));
```

Result: `LINESTRING(0 0,2 2)`.

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- Tolerance must be finite and nonnegative and is measured in coordinate units. Zero tolerance retains nonredundant source detail. Simplification can produce degenerate or invalid topology; it is not a validity repair operation. Use ST_ISVALID/ST_VALIDATE when the application requires a valid surface.

## Related Functions

[`ST_CENTROID`](st_centroid.md), [`ST_CONVEXHULL`](st_convexhull.md), [`ST_ENVELOPE`](st_envelope.md), [`ST_MAKEENVELOPE`](st_makeenvelope.md).

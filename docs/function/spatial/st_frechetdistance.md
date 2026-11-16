# `ST_FRECHETDISTANCE`

## Description

Returns the discrete Fréchet distance between two geometries.

## Syntax

```sql
ST_FRECHETDISTANCE(geometry1, geometry2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry1` | Yes | GEOMETRY | The first geometry |
| `geometry2` | Yes | GEOMETRY | The second compatible geometry |

## Return Value

Returns the discrete Fréchet distance as DOUBLE between vertices of two LineStrings in the same SRID. Empty LineStrings or SQL NULL input return NULL; other geometry types are rejected. This is not a continuous-curve distance and has no setter overload.

## Examples

```sql
SELECT ST_FRECHETDISTANCE(ST_GEOMFROMTEXT('LINESTRING(0 0,2 2)'),ST_GEOMFROMTEXT('LINESTRING(0 1,2 3)'));
```

Result:

```text
1
```

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- Work can grow with the product of the vertex counts. Fréchet also uses temporary distance state; governed memory and cancellation can reject/interrupt large inputs. Use an appropriate coordinate SRS; these vertex kernels do not provide a general ellipsoidal distance calculation.

## Related Functions

[`ST_HAUSDORFFDISTANCE`](st_hausdorffdistance.md).

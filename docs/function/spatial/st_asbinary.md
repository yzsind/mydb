# `ST_ASBINARY`

## Description

Serializes a geometry to WKB.

## Syntax

```sql
ST_ASBINARY(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns a binary string; returns `NULL` when the input is `NULL`.

## Examples

```sql
SELECT HEX(ST_ASBINARY(ST_GEOMFROMTEXT('POINT(1 2)')));
```

Result:

```text
0101000000000000000000F03F0000000000000040
```

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

- Output follows the [serializer boundary](README.md#construction-and-coordinate-order): single-geometry output form only, and the optional MySQL axis-order output argument is rejected.

## Related Functions

See [`ST_ASWKB`](st_aswkb.md).

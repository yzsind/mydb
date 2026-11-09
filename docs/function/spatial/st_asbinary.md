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

- Only the single-geometry output form is implemented. The optional MySQL axis-order output argument is not implemented and is rejected; constructor axis-order options are separate.

## Related Functions

See [`ST_ASWKB`](st_aswkb.md).

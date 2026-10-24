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

Result: Returns the hexadecimal text of the WKB for POINT(1 2).

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

See [`ST_ASWKB`](st_aswkb.md).

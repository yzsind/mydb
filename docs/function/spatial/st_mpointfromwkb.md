# `ST_MPOINTFROMWKB`

## Description

Constructs a multipoint from WKB.

## Syntax

```sql
ST_MPOINTFROMWKB(wkb [, srid] [, options])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `wkb` | Yes | WKB binary string | The WKB matching the constructor's geometry type |
| `srid` | No | Non-negative integer | The SRID of the resulting geometry; defaults to 0 |
| `options` | No | Options string | Parsing options such as axis order |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_MPOINTFROMWKB(ST_ASBINARY(ST_GEOMFROMTEXT('MULTIPOINT((0 0),(1 1))'))));
```

Result: `MULTIPOINT(0 0,1 1)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

See [`ST_MULTIPOINTFROMWKB`](st_multipointfromwkb.md).

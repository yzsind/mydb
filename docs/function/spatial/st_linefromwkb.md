# `ST_LINEFROMWKB`

## Description

Constructs a linestring from WKB.

## Syntax

```sql
ST_LINEFROMWKB(wkb [, srid] [, options])
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
SELECT ST_ASTEXT(ST_LINEFROMWKB(ST_ASBINARY(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'))));
```

Result: `LINESTRING(0 0,2 2)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

See [`ST_LINESTRINGFROMWKB`](st_linestringfromwkb.md).

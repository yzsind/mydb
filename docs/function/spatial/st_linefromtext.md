# `ST_LINEFROMTEXT`

## Description

Constructs a linestring from WKT.

## Syntax

```sql
ST_LINEFROMTEXT(wkt [, srid] [, options])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `wkt` | Yes | WKT string | The WKT matching the constructor's geometry type |
| `srid` | No | Non-negative integer | The SRID of the resulting geometry; defaults to 0 |
| `options` | No | Options string | Parsing options such as axis order |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_LINEFROMTEXT('LINESTRING(0 0, 2 2)'));
```

Result: `LINESTRING(0 0,2 2)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

See [`ST_LINESTRINGFROMTEXT`](st_linestringfromtext.md).

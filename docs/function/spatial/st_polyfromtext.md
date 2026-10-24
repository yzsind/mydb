# `ST_POLYFROMTEXT`

## Description

Constructs a polygon from WKT.

## Syntax

```sql
ST_POLYFROMTEXT(wkt [, srid] [, options])
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
SELECT ST_ASTEXT(ST_POLYFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))'));
```

Result: `POLYGON((0 0,0 2,2 2,0 0))`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

See [`ST_POLYGONFROMTEXT`](st_polygonfromtext.md).

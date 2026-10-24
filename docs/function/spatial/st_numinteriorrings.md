# `ST_NUMINTERIORRINGS`

## Description

Returns the number of interior rings of a polygon.

## Syntax

```sql
ST_NUMINTERIORRINGS(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns an integer interior-ring count for a Polygon, or SQL NULL for SQL NULL or an inapplicable geometry type.

## Examples

```sql
SELECT ST_NUMINTERIORRINGS(ST_GEOMFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))'));
```

Result: 0

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

See [`ST_NUMINTERIORRING`](st_numinteriorring.md).

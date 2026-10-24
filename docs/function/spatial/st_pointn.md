# `ST_POINTN`

## Description

Returns the Nth point in a linestring.

## Syntax

```sql
ST_POINTN(geometry, n)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | Matching composite geometry | The source geometry |
| `n` | Yes | Positive integer | The 1-based member position |

## Return Value

Returns a GEOMETRY value; returns `NULL` when any required argument is `NULL`.

## Examples

```sql
SELECT ST_ASTEXT(ST_POINTN(ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)'), 2));
```

Result: `POINT(2 2)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

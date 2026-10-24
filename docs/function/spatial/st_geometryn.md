# `ST_GEOMETRYN`

## Description

Returns the Nth geometry in a geometry collection.

## Syntax

```sql
ST_GEOMETRYN(geometry, n)
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
SELECT ST_ASTEXT(ST_GEOMETRYN(ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(POINT(1 2),POINT(2 3))'), 2));
```

Result: `POINT(2 3)`.

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

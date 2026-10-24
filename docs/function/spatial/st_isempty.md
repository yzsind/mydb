# `ST_ISEMPTY`

## Description

Determines whether a geometry is empty.

## Syntax

```sql
ST_ISEMPTY(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns `1` when the condition holds and `0` otherwise; `NULL` input usually returns `NULL`.

## Examples

```sql
SELECT ST_ISEMPTY(ST_GEOMFROMTEXT('POINT(1 2)'));
```

Result: 0

## Notes

- Geometry arguments must be valid and satisfy the function's type and SRID requirements; an incompatible SRID or geometry type returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.

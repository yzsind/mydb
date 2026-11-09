# `ST_VALIDATE`

## Description

Checks topological validity. Returns the geometry unchanged when valid, or SQL NULL when invalid; it does not repair or normalize rings.

## Syntax

```sql
ST_VALIDATE(geometry)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `geometry` | Yes | GEOMETRY matching the function's requirements | The geometry to process |

## Return Value

Returns the unchanged GEOMETRY when topologically valid; SQL NULL input or invalid topology returns NULL. Invalid encodings/SRS inputs still raise an error. It does not reorient rings or repair self-intersections.

## Examples

```sql
SELECT ST_ASTEXT(ST_VALIDATE(ST_GEOMFROMTEXT('POLYGON((0 0,2 2,0 2,2 0,0 0))')));
```

Result:

```text
NULL
```

## Notes

- See [spatial inputs and diagnostics](README.md#spatial-inputs-and-diagnostics) for encoding, SRID, empty-geometry, and geometry-type rules.

## Related Functions

[`ST_ISVALID`](st_isvalid.md), [`ST_ISSIMPLE`](st_issimple.md), [`ST_ISCLOSED`](st_isclosed.md), [`ST_ISEMPTY`](st_isempty.md).

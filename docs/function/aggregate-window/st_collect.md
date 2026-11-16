# `ST_COLLECT`

## Description

Aggregates geometries in the group into a geometry collection.

## Syntax

```sql
ST_COLLECT([DISTINCT] expr)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | Expression to evaluate. |

## Return Value

Returns a GEOMETRY collection of non-NULL geometries with a common SRID; compatible homogeneous inputs can form a Multi geometry. Empty or all-NULL input returns SQL NULL. Mixed SRIDs fail explicitly. ST_COLLECT currently has no window form.

## Examples

```sql
SELECT ST_ASTEXT(ST_COLLECT(g)) FROM (SELECT ST_GEOMFROMTEXT('POINT(1 2)') AS g UNION ALL SELECT ST_GEOMFROMTEXT('POINT(2 3)')) AS t;
```

Result: `MULTIPOINT(1 2,2 3)`.

## Notes

- Only grouped aggregation is currently supported; OVER returns an explicit unsupported error.

## Related Functions

[`JSON_ARRAYAGG`](json_arrayagg.md), [`JSON_OBJECTAGG`](json_objectagg.md), [`GROUP_CONCAT`](group_concat.md), [`GROUPING`](grouping.md).

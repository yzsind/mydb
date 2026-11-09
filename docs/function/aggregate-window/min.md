# `MIN`

## Description

Returns the minimum of non-`NULL` values in the group; also usable as a window aggregate function.

## Syntax

```sql
MIN([DISTINCT] expr)
MIN(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | Expression to evaluate. |

## Return Value

Returns the smallest non-NULL value, with argument type and collation semantics. Empty or all-NULL input returns SQL NULL. OVER finds the minimum in the frame.

## Examples

```sql
SELECT MIN(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
1
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`MAX`](max.md), [`ANY_VALUE`](any_value.md).

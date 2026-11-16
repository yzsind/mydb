# `MAX`

## Description

Returns the maximum of non-`NULL` values in the group; also usable as a window aggregate function.

## Syntax

```sql
MAX([DISTINCT] expr)
MAX(expr) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | Expression to evaluate. |

## Return Value

Returns the largest non-NULL value, with argument type and collation semantics. Empty or all-NULL input returns SQL NULL. OVER finds the maximum in the frame.

## Examples

```sql
SELECT MAX(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
2
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

## Related Functions

[`MIN`](min.md), [`ANY_VALUE`](any_value.md).

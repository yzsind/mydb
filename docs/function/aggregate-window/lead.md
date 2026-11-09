# `LEAD`

## Description

Returns the value at a specified offset after the current row within its window partition.

## Syntax

```sql
LEAD(expr [, offset [, default]]) OVER (window_spec)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any expression | The value to read |
| `offset` | No | Non-negative constant integer | Number of rows to look ahead; defaults to 1 |
| `default` | No | Constant expression | Returned when the offset is out of range; defaults to NULL |

## Return Value

Returns the argument value from the requested following partition row; the window frame does not restrict this lookup. A missing row uses the supplied default, or NULL when omitted. A present SQL NULL is not replaced by the default.

## Examples

```sql
SELECT v, LEAD(v) OVER (ORDER BY v) AS result
FROM (SELECT 10 AS v UNION ALL SELECT 20) AS t;
```

Result:

```text
10	20
20	NULL
```

## Notes

- See [window partitions and frames](README.md#window-partitions-and-frames) for defaults, peers, ordering and supported forms. Empty-input/NULL behavior follows this function's return-value rules.

- An offset of 0 reads the current row. The default is used only for an absent row, not for a present SQL NULL value. Offset must be a nonnegative constant integer; a frame clause does not restrict the partition lookup.

## Related Functions

[`FIRST_VALUE`](first_value.md), [`LAST_VALUE`](last_value.md), [`NTH_VALUE`](nth_value.md), [`LAG`](lag.md).

# `SUBTIME`

## Description

Subtracts a time value from a time or datetime.

## Syntax

```sql
SUBTIME(expr1, expr2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr1` | Yes | TIME/DATETIME or matching text | First time/datetime operand. |
| `expr2` | Yes | TIME or matching text | Amount to add/subtract. |

## Return Value

Returns a time/duration or datetime result by subtracting the second time operand from the first operand; applicable fractional precision is preserved. Invalid/overflowing inputs use temporal diagnostics. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT SUBTIME('12:00:00', '01:30:00');
```

Result: 10:30:00

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`DATE_ADD`](date_add.md), [`DATE_SUB`](date_sub.md), [`ADDTIME`](addtime.md), [`TIMESTAMPADD`](timestampadd.md).

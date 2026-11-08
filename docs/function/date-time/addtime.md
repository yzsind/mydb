# `ADDTIME`

## Description

Adds a time value to a time or datetime.

## Syntax

```sql
ADDTIME(expr1, expr2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr1` | Yes | TIME/DATETIME or matching text | First time/datetime operand. |
| `expr2` | Yes | TIME or matching text | Amount to add/subtract. |

## Return Value

Returns a time/duration result for a time first operand, or a datetime result for a datetime first operand, after adding the second time operand. Fractional precision follows the operands; overflow/conversion uses temporal diagnostics. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT ADDTIME('12:00:00', '01:30:00');
```

Result: 13:30:00

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`DATE_ADD`](date_add.md), [`DATE_SUB`](date_sub.md), [`SUBTIME`](subtime.md), [`TIMESTAMPADD`](timestampadd.md).

# `TIMEDIFF`

## Description

Returns the difference between two time or datetime values.

## Syntax

```sql
TIMEDIFF(expr1, expr2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr1` | Yes | TIME/DATETIME or matching text | First time/datetime operand. |
| `expr2` | Yes | Compatible TIME/DATETIME or matching text | Second operand; result is expr1 minus expr2. |

## Return Value

Returns a signed TIME difference expr1 minus expr2. Both operands must be interpretable in a compatible time or datetime domain. Overflow is limited by the TIME range and records the applicable warning. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT TIMEDIFF('2026-08-30 12:34:56', '2026-08-30 12:34:56');
```

Result:

```text
00:00:00
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`DATE_ADD`](date_add.md), [`DATE_SUB`](date_sub.md), [`ADDTIME`](addtime.md), [`SUBTIME`](subtime.md).

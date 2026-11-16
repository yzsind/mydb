# `TIMESTAMPDIFF`

## Description

Returns the difference between two datetimes in the specified unit.

## Syntax

```sql
TIMESTAMPDIFF(unit, datetime_expr1, datetime_expr2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `unit` | Yes | datetime unit keyword | The unit of the result |
| `datetime_expr1` | Yes | date or datetime | The starting value |
| `datetime_expr2` | Yes | date or datetime | The ending value |

## Return Value

Returns a signed integer number of complete units from datetime_expr1 to datetime_expr2. Fractional remainders are truncated toward zero; order is the reverse of DATEDIFF(date1,date2). SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT TIMESTAMPDIFF(DAY, '2026-08-30', '2026-09-01');
```

Result: 2

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

- MONTH and YEAR count complete calendar units, not a fixed number of seconds. To reverse the sign, swap the two datetime arguments.

- See [interval units](README.md#interval-units) for supported units, compound values, and operator restrictions.

## Related Functions

[`DATE_ADD`](date_add.md), [`DATE_SUB`](date_sub.md), [`ADDTIME`](addtime.md), [`SUBTIME`](subtime.md).

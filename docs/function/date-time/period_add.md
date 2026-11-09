# `PERIOD_ADD`

## Description

Adds a specified number of months to a `YYYYMM` or `YYMM` period value.

## Syntax

```sql
PERIOD_ADD(period, months)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `period` | Yes | Numeric expression | YYYYMM or YYMM period number, independent of day-of-month. |
| `months` | Yes | Numeric expression | Signed number of months. |

## Return Value

Returns an integer YYYYMM period after adding the signed month count. The input is a period number, not a DATE. A zero/invalid period raises an argument error. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT PERIOD_ADD(202608, 1);
```

Result: 202609

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`PERIOD_DIFF`](period_diff.md).

# `PERIOD_DIFF`

## Description

Returns the number of months between two period values.

## Syntax

```sql
PERIOD_DIFF(period1, period2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `period1` | Yes | Numeric expression | First YYYYMM or YYMM period. |
| `period2` | Yes | Numeric expression | Second YYYYMM or YYMM period. |

## Return Value

Returns the signed integer number of months period1 minus period2. The input periods do not contain a day component; invalid periods raise an argument error. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT PERIOD_DIFF(202609, 202608);
```

Result: 1

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`PERIOD_ADD`](period_add.md).

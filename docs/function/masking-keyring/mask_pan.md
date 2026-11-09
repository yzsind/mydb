# `MASK_PAN`

## Description

Masks digits according to payment card number rules.

## Syntax

```sql
MASK_PAN(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns text masking every character except the last four. Inputs of at most four characters are unchanged; SQL NULL returns NULL. This masks character positions, not validated payment-card digits.

## Examples

```sql
SELECT MASK_PAN('1234567890123456');
```

Result: XXXXXXXXXXXX3456

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`MASK_INNER`](mask_inner.md), [`MASK_OUTER`](mask_outer.md), [`MASK_PAN_RELAXED`](mask_pan_relaxed.md), [`MASK_SSN`](mask_ssn.md).

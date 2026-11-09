# `MASK_PAN_RELAXED`

## Description

Masks digits according to relaxed payment card number rules.

## Syntax

```sql
MASK_PAN_RELAXED(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns text retaining the first six and last four characters, masking the middle. Inputs of at most ten characters are unchanged; SQL NULL returns NULL. No payment-card validation is performed.

## Examples

```sql
SELECT MASK_PAN_RELAXED('1234567890123456');
```

Result: 123456XXXXXX3456

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`MASK_INNER`](mask_inner.md), [`MASK_OUTER`](mask_outer.md), [`MASK_PAN`](mask_pan.md), [`MASK_SSN`](mask_ssn.md).

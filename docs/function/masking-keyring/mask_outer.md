# `MASK_OUTER`

## Description

Masks both ends of a string while keeping the middle characters.

## Syntax

```sql
MASK_OUTER(str, margin1, margin2 [, mask_char])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |
| `margin1` | Yes | Integer | Number of characters at the left end. |
| `margin2` | Yes | Integer | Number of characters at the right end. |
| `mask_char` | No | String | Mask content; the current implementation takes its first character, default X. |

## Return Value

Returns text with the left/right character margins masked and the middle retained. Default mask is X; only the first supplied mask character is used. Negative margins or SQL NULL return NULL. If the margins cover the input, all characters are masked.

## Examples

```sql
SELECT MASK_OUTER('1234567890', 2, 2);
```

Result: XX345678XX

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`MASK_INNER`](mask_inner.md), [`MASK_PAN`](mask_pan.md), [`MASK_PAN_RELAXED`](mask_pan_relaxed.md), [`MASK_SSN`](mask_ssn.md).

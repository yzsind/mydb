# `MASK_INNER`

## Description

Masks the middle characters of a string while keeping both ends.

## Syntax

```sql
MASK_INNER(str, margin1, margin2 [, mask_char])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |
| `margin1` | Yes | Integer | Number of characters at the left end. |
| `margin2` | Yes | Integer | Number of characters at the right end. |
| `mask_char` | No | String | Mask content; the current implementation takes its first character, default X. |

## Return Value

Returns text with the middle masked and the two character margins retained. Default mask is X; only the first supplied mask character is used. Negative margins or SQL NULL return NULL. If margins cover the whole input, it is returned unchanged.

## Examples

```sql
SELECT MASK_INNER('1234567890', 2, 2);
```

Result: 12XXXXXX90

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`MASK_OUTER`](mask_outer.md), [`MASK_PAN`](mask_pan.md), [`MASK_PAN_RELAXED`](mask_pan_relaxed.md), [`MASK_SSN`](mask_ssn.md).

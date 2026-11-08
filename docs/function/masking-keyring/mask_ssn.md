# `MASK_SSN`

## Description

Masks digits according to social security number rules.

## Syntax

```sql
MASK_SSN(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns XXX-XX- followed by the last four characters of an eleven-character input. SQL NULL returns NULL; any other character count raises error 1123. It checks length, not full SSN digit validity.

## Examples

```sql
SELECT MASK_SSN('123-45-6789');
```

Result:

```text
XXX-XX-6789
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`MASK_INNER`](mask_inner.md), [`MASK_OUTER`](mask_outer.md), [`MASK_PAN`](mask_pan.md), [`MASK_PAN_RELAXED`](mask_pan_relaxed.md).

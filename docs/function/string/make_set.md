# `MAKE_SET`

## Description

Selects arguments according to a bitmap and joins them into a comma-separated string.

## Syntax

```sql
MAKE_SET(bits, str1 [, str2] ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `bits` | Yes | Integer | Bitmap read from bit 0. |
| `str1 ...` | Yes, repeatable | String or NULL | Candidate strings; NULL candidates are omitted. |

## Return Value

Returns comma-separated selected strings. Bit 0 selects str1, bit 1 selects str2, and so on. NULL candidate strings are skipped, while a NULL bitmap returns NULL. No selected strings returns empty text.

## Examples

```sql
SELECT MAKE_SET(5, 'a', 'b', 'c');
```

Result: a,c

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`EXPORT_SET`](export_set.md), [`LIST_ADD`](list_add.md), [`LIST_DROP`](list_drop.md), [`FORMAT`](format.md).

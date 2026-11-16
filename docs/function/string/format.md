# `FORMAT`

## Description

Formats a number with a given number of decimals and an optional locale.

## Syntax

```sql
FORMAT(number, decimals [, locale])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric or integer | Numeric operand; strings use numeric conversion. |
| `decimals` | Yes | Numeric or integer | Decimal places; a negative value operates to the left of the decimal point. |
| `locale` | No | String or binary string | Locale name; omitted locale uses en_US. |

## Return Value

Returns formatted text with grouping separators and the requested decimal digits. decimals is clamped to 0..30; omitted locale uses en_US. SQL NULL number/decimals returns NULL. An unknown locale uses the fallback locale with a diagnostic.

## Examples

```sql
SELECT FORMAT(1234.5, 2);
```

Result: 1,234.50

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`EXPORT_SET`](export_set.md), [`MAKE_SET`](make_set.md), [`LIST_ADD`](list_add.md), [`LIST_DROP`](list_drop.md).

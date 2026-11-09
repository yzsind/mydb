# `EXPORT_SET`

## Description

Selects the on/off strings according to an integer bitmap and concatenates the result.

## Syntax

```sql
EXPORT_SET(bits, on, off [, separator [, number_of_bits]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `bits` | Yes | Integer | Bitmap read from least significant bit first. |
| `on` | Yes | String | Output for a set bit. |
| `off` | Yes | String | Output for an unset bit. |
| `separator` | No | String | Separator, default comma. |
| `number_of_bits` | No | Integer | Number of bits to emit, default 64 and capped at 64. |

## Return Value

Returns text selected bit by bit, from the least significant bit first. Set bits emit on and unset bits emit off. Separator defaults to comma and number_of_bits to 64, capped at 64. Any supplied SQL NULL argument returns NULL.

## Examples

```sql
SELECT EXPORT_SET(5, 'Y', 'N', ',', 3);
```

Result: Y,N,Y

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`MAKE_SET`](make_set.md), [`LIST_ADD`](list_add.md), [`LIST_DROP`](list_drop.md), [`FORMAT`](format.md).

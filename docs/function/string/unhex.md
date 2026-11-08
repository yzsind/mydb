# `UNHEX`

## Description

Decodes hexadecimal text into a byte string.

## Syntax

```sql
UNHEX(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns binary bytes decoded from hexadecimal text. SQL NULL or invalid hexadecimal input returns NULL. HEX in the example displays the bytes without character-encoding ambiguity.

## Examples

```sql
SELECT HEX(UNHEX('6D796462'));
```

Result:

```text
6D796462
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

- Odd-length hexadecimal input is interpreted with a leading zero nibble: HEX(UNHEX('F')) returns 0F. Non-hexadecimal input returns NULL; this function returns bytes, not decoded Unicode text.

## Related Functions

[`HEX`](hex.md), [`QUOTE`](quote.md), [`QUOTE_IDENTIFIER`](quote_identifier.md).

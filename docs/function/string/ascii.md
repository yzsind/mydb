# `ASCII`

## Description

Returns the ASCII code of the first character of a string.

## Syntax

```sql
ASCII(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns the unsigned value of the first encoded byte, or 0 for an empty string. SQL NULL returns NULL. For a multibyte first character this is not the Unicode code point; compare ORD.

## Examples

```sql
SELECT ASCII(''), ASCII('A');
```

Result:

```text
0	65
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`LENGTH`](length.md), [`CHAR_LENGTH`](char_length.md), [`BIT_LENGTH`](bit_length.md), [`ORD`](ord.md).

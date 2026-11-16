# `ORD`

## Description

Returns the multi-byte encoded value of the first character of a string.

## Syntax

```sql
ORD(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns the integer formed by the first character's encoded bytes in big-endian base 256; an empty string returns 0. SQL NULL returns NULL. The result depends on the character encoding.

## Examples

```sql
SELECT ORD('mydb');
```

Result:

```text
109
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`LENGTH`](length.md), [`CHAR_LENGTH`](char_length.md), [`BIT_LENGTH`](bit_length.md), [`ASCII`](ascii.md).

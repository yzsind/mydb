# `BIT_LENGTH`

## Description

Returns the length of a string in bits.

## Syntax

```sql
BIT_LENGTH(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns the encoded byte length multiplied by 8, as an integer. SQL NULL returns NULL; this does not count Unicode characters.

## Examples

```sql
SELECT BIT_LENGTH('mydb');
```

Result:

```text
32
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`LENGTH`](length.md), [`CHAR_LENGTH`](char_length.md), [`ASCII`](ascii.md), [`ORD`](ord.md).

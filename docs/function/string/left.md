# `LEFT`

## Description

Returns the leftmost given number of characters of a string.

## Syntax

```sql
LEFT(str, len)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |
| `len` | Yes | Integer | Requested length; text uses characters and binary input uses bytes. |

## Return Value

Returns the first len characters of text, or len bytes of binary input. Nonpositive len returns an empty result; SQL NULL input returns NULL.

## Examples

```sql
SELECT LEFT('mydb', 2);
```

Result:

```text
my
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`SUBSTRING`](substring.md), [`RIGHT`](right.md), [`INSERT`](insert.md), [`SUBSTRING_INDEX`](substring_index.md).

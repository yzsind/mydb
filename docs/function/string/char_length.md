# `CHAR_LENGTH`

## Description

Returns the number of characters in a string.

## Syntax

```sql
CHAR_LENGTH(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns the character count as an integer for text, or the byte count for binary strings. SQL NULL returns NULL. Combining characters are counted separately, not as one user-perceived grapheme.

## Examples

```sql
SELECT CHAR_LENGTH('é'), LENGTH('é');
```

Result:

```text
1	2
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

- [`CHARACTER_LENGTH`](character_length.md): alias of this function.

# `SUBSTRING_INDEX`

## Description

Returns one side of a string split by a delimiter occurrence count.

## Syntax

```sql
SUBSTRING_INDEX(str, delimiter, count)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |
| `delimiter` | Yes | String or binary string | Delimiter whose occurrences are counted. |
| `count` | Yes | Integer | Repetition count. |

## Return Value

Returns the prefix before count delimiters when count is positive, or the suffix after -count delimiters when negative. Zero count or empty delimiter returns an empty string; too few delimiters preserves the source. SQL NULL input returns NULL. Delimiter matching is case-sensitive.

## Examples

```sql
SELECT SUBSTRING_INDEX('a/b/c','/',2), SUBSTRING_INDEX('a/b/c','/',-1);
```

Result:

```text
a/b	c
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`SUBSTRING`](substring.md), [`LEFT`](left.md), [`RIGHT`](right.md), [`INSERT`](insert.md).

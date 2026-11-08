# `SOUNDEX`

## Description

Returns the Soundex phonetic code of a string.

## Syntax

```sql
SOUNDEX(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns a phonetic Soundex text code for the input. SQL NULL returns NULL. It is an approximate name-matching aid rather than a unique identifier or Unicode transliteration.

## Examples

```sql
SELECT SOUNDEX('mydb');
```

Result:

```text
M310
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`LOWER`](lower.md), [`UPPER`](upper.md).

# `REPLACE`

## Description

Replaces all occurrences of a given substring within a string with new content.

## Syntax

```sql
REPLACE(str, from_str, to_str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |
| `from_str` | Yes | String | Substring to replace; matching is case-sensitive. |
| `to_str` | Yes | String | Replacement content. |

## Return Value

Returns text/binary content with every case-sensitive occurrence of from_str replaced by to_str. An empty from_str preserves the source. Any SQL NULL argument returns NULL.

## Examples

```sql
SELECT REPLACE('mydb', 'db', 'x');
```

Result:

```text
myx
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`REGEXP_REPLACE`](regexp_replace.md), [`REGEXP_SUBSTR`](regexp_substr.md), [`REGEXP_INSTR`](regexp_instr.md), [`REGEXP_LIKE`](regexp_like.md).

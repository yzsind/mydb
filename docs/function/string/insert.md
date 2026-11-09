# `INSERT`

## Description

Replaces a portion of a string starting at a specified position.

## Syntax

```sql
INSERT(str, pos, len, newstr)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |
| `pos` | Yes | Numeric or integer | One-based starting position; negative positions are supported only by SUBSTRING and its aliases. |
| `len` | Yes | Integer | Requested length; text uses characters and binary input uses bytes. |
| `newstr` | Yes | String | Replacement string. |

## Return Value

Returns the source string with len characters/bytes replaced at one-based pos. Invalid positions preserve the source; a length extending beyond the end replaces the remaining suffix. A negative len replaces the suffix. Any SQL NULL argument returns NULL.

## Examples

```sql
SELECT INSERT('mydb', 2, 2, 'x');
```

Result:

```text
mxb
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`SUBSTRING`](substring.md), [`LEFT`](left.md), [`RIGHT`](right.md), [`SUBSTRING_INDEX`](substring_index.md).

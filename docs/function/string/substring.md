# `SUBSTRING`

## Description

Extracts a substring from a specified position, optionally with a given length.

## Syntax

```sql
SUBSTRING(str, pos)
SUBSTRING(str, pos, len)
SUBSTRING(str FROM pos [FOR len])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |
| `pos` | Yes | Numeric or integer | One-based starting position; negative positions are supported only by SUBSTRING and its aliases. |
| `len` | No | Integer | Requested length; text uses characters and binary input uses bytes. |

## Return Value

Returns text/binary content from one-based pos, optionally limited by len. A negative pos counts backward from the end; pos=0 or nonpositive len returns an empty result. SQL NULL in a supplied argument returns NULL.

## Examples

```sql
SELECT SUBSTRING('mydb',3,2), SUBSTRING('mydb',-2), LENGTH(SUBSTRING('mydb',0));
```

Result:

```text
db	db	0
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

- [`MID`](mid.md), [`SUBSTR`](substr.md): aliases of this function.

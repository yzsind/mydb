# `LOWER`

## Description

Converts a string to lowercase.

## Syntax

```sql
LOWER(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns lowercase text using Unicode case mapping. Binary input has no character-case conversion. SQL NULL returns NULL.

## Examples

```sql
SELECT LOWER('MyDB');
```

Result: mydb

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

- [`LCASE`](lcase.md): alias of this function.

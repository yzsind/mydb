# `LENGTH`

## Description

Returns the length of a string in bytes.

## Syntax

```sql
LENGTH(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns the encoded byte count as an integer. SQL NULL returns NULL; an empty string has length 0.

## Examples

```sql
SELECT LENGTH('mydb');
```

Result: 4

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

- [`OCTET_LENGTH`](octet_length.md): alias of this function.

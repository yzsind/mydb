# `UPPER`

## Description

Converts a string to uppercase.

## Syntax

```sql
UPPER(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns uppercase text using Unicode case mapping. Binary input has no character-case conversion. SQL NULL returns NULL.

## Examples

```sql
SELECT UPPER('mydb');
```

Result: MYDB

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

- [`UCASE`](ucase.md): alias of this function.

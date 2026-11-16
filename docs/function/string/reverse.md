# `REVERSE`

## Description

Reverses a string.

## Syntax

```sql
REVERSE(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns text in reverse character order, or binary data in reverse byte order. SQL NULL returns NULL; combining marks are reversed independently.

## Examples

```sql
SELECT REVERSE('mydb');
```

Result:

```text
bdym
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[Category reference](README.md).

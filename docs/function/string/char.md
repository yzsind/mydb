# `CHAR`

## Description

Converts a sequence of integer code values into a string.

## Syntax

```sql
CHAR(n [, n] ... [USING charset_name])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `n ...` | Yes, repeatable | Integer or convertible numeric expression | Code bytes; NULL arguments are skipped. |
| `charset_name` | No | Charset name | USING requests conversion of the resulting bytes; default is binary. |

## Return Value

Returns binary bytes by default, or text in the charset selected by USING. NULL integer arguments are skipped; at least one integer argument is required. Integers above 255 produce multiple bytes, not a Unicode code point conversion. Invalid encoded output under USING follows charset conversion diagnostics.

## Examples

```sql
SELECT HEX(CHAR(65,NULL,256));
```

Result:

```text
410100
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`LENGTH`](length.md), [`CHAR_LENGTH`](char_length.md), [`BIT_LENGTH`](bit_length.md), [`ASCII`](ascii.md).

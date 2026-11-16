# `HEX`

## Description

Returns a hexadecimal representation of a string's bytes or an integer.

## Syntax

```sql
HEX(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Numeric, text or binary expression | Numbers use integer formatting; text/binary content is encoded byte by byte. |

## Return Value

Returns uppercase hexadecimal text; numeric input is formatted as an integer, while string/binary input is encoded byte by byte. SQL NULL returns NULL.

## Examples

```sql
SELECT HEX(2);
```

Result:

```text
2
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`UNHEX`](unhex.md), [`QUOTE`](quote.md), [`QUOTE_IDENTIFIER`](quote_identifier.md).

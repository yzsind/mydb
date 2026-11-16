# `QUOTE_IDENTIFIER`

## Description

Quotes and escapes an SQL identifier.

## Syntax

```sql
QUOTE_IDENTIFIER(identifier)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `identifier` | Yes | String | One identifier to quote; embedded backticks are doubled. |

## Return Value

Returns text enclosed in backticks, doubling each embedded backtick. SQL NULL returns NULL. Quote the components of a qualified name separately.

## Examples

```sql
SELECT QUOTE_IDENTIFIER('a`b');
```

Result:

```text
`a``b`
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`HEX`](hex.md), [`UNHEX`](unhex.md), [`QUOTE`](quote.md).

# `QUOTE`

## Description

Escapes a string into single-quoted text suitable for embedding in SQL.

## Syntax

```sql
QUOTE(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns SQL-quoted text with escape sequences. SQL NULL returns the text `NULL` rather than SQL NULL.

## Examples

```sql
SELECT QUOTE('mydb');
```

Result:

```text
'mydb'
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.
- `QUOTE(NULL)` returns the string `NULL`, not SQL NULL.

- The returned literal escapes quote, backslash, NUL and Ctrl-Z using backslash escapes. Under NO_BACKSLASH_ESCAPES that returned text is not a generally reusable SQL literal; pass data through prepared placeholders instead of concatenating SQL.
- The string `NULL` returned for SQL NULL is distinct from a SQL NULL result.

## Related Functions

[`HEX`](hex.md), [`UNHEX`](unhex.md), [`QUOTE_IDENTIFIER`](quote_identifier.md).

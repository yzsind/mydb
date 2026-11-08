# `CONCAT_WS`

## Description

Concatenates the remaining non-`NULL` arguments using the first argument as the separator.

## Syntax

```sql
CONCAT_WS(separator, str1 [, str2 [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `separator` | Yes | String or binary string | The separator used in the concatenated result |
| `str1` | Yes | String | First string. |
| `str2 ...` | No, repeatable | String | Additional strings, in argument order. |

## Return Value

Returns text/binary concatenation separated by the first argument. A NULL separator returns NULL; NULL value arguments are skipped rather than making the entire result NULL.

## Examples

```sql
SELECT CONCAT_WS(':','a',NULL,'','b');
```

Result:

```text
a::b
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`CONCAT`](concat.md), [`REPEAT`](repeat.md), [`SPACE`](space.md).

# `CONCAT`

## Description

Concatenates all arguments in order.

## Syntax

```sql
CONCAT(str1 [, str2 [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str1` | Yes | String | First string. |
| `str2 ...` | No, repeatable | String | Additional strings, in argument order. |

## Return Value

Returns concatenated text, or binary bytes when binary input determines the result domain. Any SQL NULL argument returns NULL. Empty strings contribute no bytes; numeric arguments are converted to text.

## Examples

```sql
SELECT CONCAT('my', 'db');
```

Result: mydb

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`CONCAT_WS`](concat_ws.md), [`REPEAT`](repeat.md), [`SPACE`](space.md).

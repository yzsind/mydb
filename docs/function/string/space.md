# `SPACE`

## Description

Returns a string of a given number of spaces.

## Syntax

```sql
SPACE(count)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `count` | Yes | Integer or convertible number | Number of ASCII spaces; nonpositive values produce empty text. |

## Return Value

Returns a text string containing count ASCII spaces. Nonpositive count returns an empty string; SQL NULL returns NULL. Output beyond the packet limit returns NULL with a warning.

## Examples

```sql
SELECT LENGTH(SPACE(3)), HEX(SPACE(3));
```

Result:

```text
3	202020
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`CONCAT`](concat.md), [`CONCAT_WS`](concat_ws.md), [`REPEAT`](repeat.md).

# `REPEAT`

## Description

Repeats a string a given number of times.

## Syntax

```sql
REPEAT(str, count)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |
| `count` | Yes | Integer | Repetition count. |

## Return Value

Returns text/binary content repeated count times. A nonpositive count returns an empty result; SQL NULL returns NULL. Excessive output returns NULL with a packet-size warning, or a resource error.

## Examples

```sql
SELECT REPEAT('mydb', 2);
```

Result:

```text
mydbmydb
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`CONCAT`](concat.md), [`CONCAT_WS`](concat_ws.md), [`SPACE`](space.md).

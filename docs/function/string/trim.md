# `TRIM`

## Description

Removes spaces or a given character sequence from both ends or a specified direction of a string.

## Syntax

```sql
TRIM([[BOTH | LEADING | TRAILING] [remstr] FROM] str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The string to trim |
| `direction` | No | BOTH, LEADING, or TRAILING | The trimming direction; defaults to BOTH |
| `remstr` | No | String | The content to remove; defaults to spaces |

## Return Value

Returns text/binary content after removing repeated whole remstr sequences at the requested end(s). remstr defaults to one space and direction to BOTH. It is substring removal, not removal of an arbitrary character set. SQL NULL input returns NULL.

## Examples

```sql
SELECT TRIM(BOTH 'x' FROM 'xmydbx');
```

Result: mydb

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`LPAD`](lpad.md), [`RPAD`](rpad.md), [`LTRIM`](ltrim.md), [`RTRIM`](rtrim.md).

# `LPAD`

## Description

Left-pads a string to a specified length.

## Syntax

```sql
LPAD(str, len, padstr)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |
| `len` | Yes | Integer | Requested length; text uses characters and binary input uses bytes. |
| `padstr` | Yes | String | Repeated padding content; all three arguments are required. |

## Return Value

Returns text/binary content of len characters/bytes, padding at the left by repeating padstr or truncating the source. SQL NULL or negative len returns NULL; extension with an empty padstr returns an empty string. All three arguments are required.

## Examples

```sql
SELECT LPAD('mydb', 6, '0');
```

Result:

```text
00mydb
```

## Notes

- All three arguments are required, even when `len` would truncate the input; missing `padstr` returns 1582/42000.
- A NULL argument returns SQL NULL. A negative length returns NULL; an empty pad string returns an empty string when extension would be needed.

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`RPAD`](rpad.md), [`LTRIM`](ltrim.md), [`RTRIM`](rtrim.md), [`TRIM`](trim.md).

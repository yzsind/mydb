# `RTRIM`

## Description

Removes trailing spaces from a string.

## Syntax

```sql
RTRIM(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |

## Return Value

Returns text/binary content after removing trailing space characters (0x20). Other whitespace is retained. SQL NULL returns NULL.

## Examples

```sql
SELECT CONCAT('[',RTRIM('  mydb  '),']');
```

Result:

```text
[  mydb]
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`LPAD`](lpad.md), [`RPAD`](rpad.md), [`LTRIM`](ltrim.md), [`TRIM`](trim.md).

# `STRCMP`

## Description

Compares two strings and returns -1, 0, or 1.

## Syntax

```sql
STRCMP(str1, str2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str1` | Yes | String | First string. |
| `str2` | Yes | String | Second string. |

## Return Value

Returns integer -1, 0, or 1 according to the resolved string comparison/collation, or SQL NULL for NULL input.

## Examples

```sql
SELECT STRCMP('a','b'), STRCMP('b','a'), STRCMP('a','a');
```

Result:

```text
-1	1	0
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`LOCATE`](locate.md), [`INSTR`](instr.md), [`POSITION`](position.md), [`FIND_IN_SET`](find_in_set.md).

# `LOCATE`

## Description

Returns the position of a substring within a string, optionally from a given starting position.

## Syntax

```sql
LOCATE(substr, str [, pos])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `substr` | Yes | String | Substring to locate. |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |
| `pos` | No | Numeric or integer | One-based starting position; negative positions are supported only by SUBSTRING and its aliases. |

## Return Value

Returns the one-based match position in str, searching from pos (default 1). No match or a start below 1 returns 0; SQL NULL input returns NULL. Text positions count characters and binary positions count bytes.

## Examples

```sql
SELECT LOCATE('db', 'mydb');
```

Result: 3

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`INSTR`](instr.md), [`POSITION`](position.md), [`FIND_IN_SET`](find_in_set.md), [`STRCMP`](strcmp.md).

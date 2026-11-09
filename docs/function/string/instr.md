# `INSTR`

## Description

Returns the position of the first occurrence of a substring within a string.

## Syntax

```sql
INSTR(str, substr)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |
| `substr` | Yes | String | Substring to locate. |

## Return Value

Returns the one-based position of substr in str, 0 when not found, or SQL NULL for NULL input. Text uses character positions and resolved comparison semantics; binary strings use bytes.

## Examples

```sql
SELECT INSTR('mydb', 'db');
```

Result: 3

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`LOCATE`](locate.md), [`POSITION`](position.md), [`FIND_IN_SET`](find_in_set.md), [`STRCMP`](strcmp.md).
